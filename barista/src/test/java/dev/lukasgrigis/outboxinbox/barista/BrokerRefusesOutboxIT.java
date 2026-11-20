package dev.lukasgrigis.outboxinbox.barista;

import dev.lukasgrigis.outboxinbox.barista.service.SaleService;
import dev.lukasgrigis.outboxinbox.messaging.ContentHash;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.rabbitmq.client.RabbitAmqpAdmin;
import org.springframework.amqp.rabbitmq.client.RabbitAmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class BrokerRefusesOutboxIT extends AbstractBaristaIT {

    @Autowired
    public BrokerRefusesOutboxIT(
        SaleService sales,
        TransactionTemplate transaction,
        JdbcTemplate jdbc,
        RabbitAmqpAdmin admin,
        RabbitAmqpTemplate amqp,
        Declarables topology
    ) {
        super(sales, transaction, jdbc, admin, amqp, topology);
    }

    @Test
    void failsThePublicationAndResendsTheSameMessageOnceTheBrokerAccepts() throws Exception {
        refuseMessages();
        final var receipt = sales.sell("card-1", 3);
        awaitPublication("FAILED");
        assertThat(queueDepth()).isZero();

        acceptMessages();
        // OutboxRelay resubmits it within barista.resubmit-interval
        awaitPublication("COMPLETED");

        final var message = receive();
        final var stored = jdbc.queryForObject("SELECT serialized_event FROM event_publication", String.class);
        final var postgresSha256 = jdbc.queryForObject(
            "SELECT encode(sha256(convert_to(serialized_event, 'UTF8')), 'hex') FROM event_publication",
            String.class
        );
        assertThat(message.getMessageProperties().getMessageId()).isEqualTo(receipt.messageId().toString());
        assertThat(message.getBody()).isEqualTo(stored.getBytes(StandardCharsets.UTF_8));
        assertThat(message.getMessageProperties().<String>getHeader(ContentHash.HEADER)).isEqualTo(postgresSha256);
    }

    private void awaitPublication(String status) {
        await().atMost(Duration.ofSeconds(20)).untilAsserted(
            () -> assertThat(jdbc.queryForObject("SELECT status FROM event_publication", String.class))
                .isEqualTo(status)
        );
    }

}
