package dev.lukasgrigis.outboxinbox.loyalty;

import com.rabbitmq.client.amqp.Environment;
import dev.lukasgrigis.outboxinbox.messaging.CoffeeSoldTopology;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.amqp.rabbitmq.client.RabbitAmqpTemplate;
import org.springframework.amqp.rabbitmq.client.SingleAmqpConnectionFactory;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.Network;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.rabbitmq.RabbitMQContainer;
import org.testcontainers.toxiproxy.ToxiproxyContainer;

import java.util.concurrent.TimeUnit;

import static dev.lukasgrigis.outboxinbox.loyalty.AbstractLoyaltyIT.coffeeSold;
import static dev.lukasgrigis.outboxinbox.loyalty.AbstractLoyaltyIT.message;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

// loyalty reaches the broker through Toxiproxy, which can swallow a settlement after the stamp is committed
@SpringBootTest
@ExtendWith(OutputCaptureExtension.class)
class LostSettlementIT {

    static final Network NETWORK = Network.newNetwork();

    @ServiceConnection
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-trixie");

    static final RabbitMQContainer RABBITMQ = AbstractLoyaltyIT.rabbitmq()
        .withNetwork(NETWORK)
        .withNetworkAliases("rabbitmq");

    static final ToxiproxyContainer TOXIPROXY = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.13.0")
        .withNetwork(NETWORK)
        .withExposedPorts(8474, 8666);

    static final Proxy PROXY = proxy();

    private final JdbcTemplate jdbc;
    private final Environment environment;
    private final RabbitListenerEndpointRegistry registry;

    @Autowired
    public LostSettlementIT(
        JdbcTemplate jdbc,
        Environment environment,
        RabbitListenerEndpointRegistry registry
    ) {
        this.jdbc = jdbc;
        this.environment = environment;
        this.registry = registry;
    }

    @SneakyThrows
    static Proxy proxy() {
        RABBITMQ.start();
        TOXIPROXY.start();
        return new ToxiproxyClient(TOXIPROXY.getHost(), TOXIPROXY.getControlPort())
            .createProxy("rabbitmq", "0.0.0.0:8666", "rabbitmq:5672");
    }

    @DynamicPropertySource
    static void throughToxiproxy(DynamicPropertyRegistry registry) {
        registry.add("spring.amqp.rabbitmq.host", TOXIPROXY::getHost);
        registry.add("spring.amqp.rabbitmq.port", () -> TOXIPROXY.getMappedPort(8666));
    }

    @Test
    void theBrokerRedeliversAndTheInboxSuppressesTheDuplicate(CapturedOutput output) throws Exception {
        // from here on nothing loyalty sends reaches the broker: the stamp commits, its accept is lost
        PROXY.toxics().timeout("settlement-lost", ToxicDirection.UPSTREAM, 0);
        final var coffeeSold = coffeeSold(2);
        // straight to the broker, around the proxy
        final var direct = new SingleAmqpConnectionFactory(environment)
            .setHost(RABBITMQ.getHost())
            .setPort(RABBITMQ.getAmqpPort());
        new RabbitAmqpTemplate(direct)
            .send(CoffeeSoldTopology.EXCHANGE, CoffeeSoldTopology.ROUTING_KEY, message(coffeeSold))
            .get(10, TimeUnit.SECONDS);
        await().untilAsserted(() -> assertThat(stamps()).isOne());

        // the connection dies with the delivery unsettled
        PROXY.disable();
        PROXY.toxics().get("settlement-lost").remove();
        PROXY.enable();

        // delivery 1, not 0: the broker counted the dead connection as a failed delivery
        await().untilAsserted(
            () -> assertThat(output).contains("Message " + coffeeSold.messageId() + " duplicate on delivery 1")
        );
        assertThat(stamps()).isOne();
    }

    // no getter on the container; Boot applies the ContainerCustomizer only while it is the only one
    @Test
    void theListenerContainerTakesOneMessageAtATime() {
        assertThat(registry.getListenerContainers())
            .singleElement()
            .extracting(container -> new DirectFieldAccessor(container).getPropertyValue("initialCredits"))
            .isEqualTo(1);
    }

    private int stamps() {
        return jdbc.queryForObject("SELECT COUNT(*) FROM stamp", Integer.class);
    }

}
