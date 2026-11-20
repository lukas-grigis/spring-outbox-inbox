package dev.lukasgrigis.outboxinbox.loyalty;

import dev.lukasgrigis.outboxinbox.messaging.CoffeeSold;
import dev.lukasgrigis.outboxinbox.messaging.CoffeeSoldTopology;
import dev.lukasgrigis.outboxinbox.messaging.ContentHash;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbitmq.client.RabbitAmqpAdmin;
import org.springframework.amqp.rabbitmq.client.RabbitAmqpTemplate;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.rabbitmq.RabbitMQContainer;
import org.testcontainers.utility.MountableFile;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

// a fresh context per class: two live contexts would both consume the queue
@SpringBootTest
@DirtiesContext
abstract class AbstractLoyaltyIT {

    static final String CARD = "card-1";

    @ServiceConnection
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-trixie");

    @ServiceConnection
    static final RabbitMQContainer RABBITMQ = rabbitmq();

    private static final JacksonJsonMessageConverter CONVERTER = new JacksonJsonMessageConverter();

    protected final JdbcTemplate jdbc;
    protected final RabbitAmqpTemplate amqp;
    protected final RabbitAmqpAdmin admin;

    @Autowired
    public AbstractLoyaltyIT(
        JdbcTemplate jdbc,
        RabbitAmqpTemplate amqp,
        RabbitAmqpAdmin admin
    ) {
        this.jdbc = jdbc;
        this.amqp = amqp;
        this.admin = admin;
    }

    // dead-lettering and the delivery limit are broker policy, as in the chaos run
    static RabbitMQContainer rabbitmq() {
        return new RabbitMQContainer("rabbitmq:4.4.1-management")
            .withCopyFileToContainer(
                MountableFile.forHostPath("../support/rabbitmq/rabbitmq.conf"),
                "/etc/rabbitmq/conf.d/20-outbox-inbox.conf"
            )
            .withCopyFileToContainer(
                MountableFile.forHostPath("../support/rabbitmq/definitions.json"),
                "/etc/rabbitmq/definitions.json"
            )
            .withStartupTimeout(Duration.ofMinutes(2));
    }

    static CoffeeSold coffeeSold(int cups) {
        return new CoffeeSold(UUID.randomUUID(), UUID.randomUUID(), CARD, cups, Instant.now());
    }

    // as barista sends it: the message id property and the hash of the body in a header
    static Message message(CoffeeSold coffeeSold) {
        final var message = CONVERTER.toMessage(coffeeSold, new MessageProperties());
        message.getMessageProperties().setMessageId(coffeeSold.messageId().toString());
        message.getMessageProperties().setHeader(ContentHash.HEADER, ContentHash.of(message.getBody()));
        return message;
    }

    @BeforeEach
    void clean() {
        jdbc.execute("TRUNCATE stamp, inbox");
        admin.purgeQueue(CoffeeSoldTopology.QUEUE);
        admin.purgeQueue(CoffeeSoldTopology.PARKING_QUEUE);
    }

    void send(Message message) throws Exception {
        amqp.send(CoffeeSoldTopology.EXCHANGE, CoffeeSoldTopology.ROUTING_KEY, message).get(10, TimeUnit.SECONDS);
    }

    int cardTotal() {
        return jdbc.queryForObject("SELECT COALESCE(SUM(stamps), 0) FROM stamp WHERE card_id = ?", Integer.class, CARD);
    }

}
