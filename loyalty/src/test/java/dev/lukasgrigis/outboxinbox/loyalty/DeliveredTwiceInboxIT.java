package dev.lukasgrigis.outboxinbox.loyalty;

import dev.lukasgrigis.outboxinbox.messaging.ContentHash;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbitmq.client.RabbitAmqpAdmin;
import org.springframework.amqp.rabbitmq.client.RabbitAmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class DeliveredTwiceInboxIT extends AbstractLoyaltyIT {

    @Autowired
    public DeliveredTwiceInboxIT(
        JdbcTemplate jdbc,
        RabbitAmqpTemplate amqp,
        RabbitAmqpAdmin admin
    ) {
        super(jdbc, amqp, admin);
    }

    @Test
    void stampsOnceAndCountsOneDuplicate() throws Exception {
        final var message = message(coffeeSold(3));

        send(message);
        send(message);

        await().untilAsserted(
            () -> assertThat(jdbc.queryForList("SELECT duplicates FROM inbox", Integer.class)).containsExactly(1)
        );
        assertThat(cardTotal()).isEqualTo(3);
        // Postgres sha256() over the stored body equals the producer's hash
        assertThat(jdbc.queryForObject(
            "SELECT ENCODE(sha256(CONVERT_TO(body, 'UTF8')), 'hex') FROM stamp",
            String.class
        )).isEqualTo(message.getMessageProperties().getHeader(ContentHash.HEADER));
    }

}
