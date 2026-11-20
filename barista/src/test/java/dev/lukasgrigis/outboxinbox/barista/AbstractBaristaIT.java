package dev.lukasgrigis.outboxinbox.barista;

import dev.lukasgrigis.outboxinbox.barista.service.SaleService;
import dev.lukasgrigis.outboxinbox.messaging.CoffeeSoldTopology;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbitmq.client.RabbitAmqpAdmin;
import org.springframework.amqp.rabbitmq.client.RabbitAmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.rabbitmq.RabbitMQContainer;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@SpringBootTest
abstract class AbstractBaristaIT {

    @ServiceConnection
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-trixie");

    @ServiceConnection
    static final RabbitMQContainer RABBITMQ = new RabbitMQContainer("rabbitmq:4.4.1-management")
        .withStartupTimeout(Duration.ofMinutes(2));

    protected final SaleService sales;
    protected final TransactionTemplate transaction;
    protected final JdbcTemplate jdbc;
    private final RabbitAmqpAdmin admin;
    private final RabbitAmqpTemplate amqp;
    private final Declarables topology;

    @Autowired
    public AbstractBaristaIT(
        SaleService sales,
        TransactionTemplate transaction,
        JdbcTemplate jdbc,
        RabbitAmqpAdmin admin,
        RabbitAmqpTemplate amqp,
        Declarables topology
    ) {
        this.sales = sales;
        this.transaction = transaction;
        this.jdbc = jdbc;
        this.admin = admin;
        this.amqp = amqp;
        this.topology = topology;
    }

    @BeforeEach
    void clean() {
        jdbc.execute("TRUNCATE sale, event_publication");
        acceptMessages();
        admin.purgeQueue(CoffeeSoldTopology.QUEUE);
    }

    // without its binding the exchange cannot route, so the broker answers RELEASED instead of ACCEPTED
    void refuseMessages() {
        admin.removeBinding(binding());
    }

    void acceptMessages() {
        admin.declareBinding(binding());
    }

    private Binding binding() {
        return topology.getDeclarablesByType(Binding.class).getFirst();
    }

    int count(String table) {
        return jdbc.queryForObject("SELECT COUNT(*) FROM " + table, Integer.class);
    }

    long queueDepth() {
        return admin.getQueueInfo(CoffeeSoldTopology.QUEUE).getMessageCount();
    }

    Message receive() throws Exception {
        return amqp.receive(CoffeeSoldTopology.QUEUE).get(10, TimeUnit.SECONDS);
    }

}
