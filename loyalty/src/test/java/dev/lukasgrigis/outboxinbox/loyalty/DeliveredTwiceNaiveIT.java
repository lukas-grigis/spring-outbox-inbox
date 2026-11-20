package dev.lukasgrigis.outboxinbox.loyalty;

import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbitmq.client.RabbitAmqpAdmin;
import org.springframework.amqp.rabbitmq.client.RabbitAmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(properties = "loyalty.mode=naive")
class DeliveredTwiceNaiveIT extends AbstractLoyaltyIT {

    @Autowired
    public DeliveredTwiceNaiveIT(
        JdbcTemplate jdbc,
        RabbitAmqpTemplate amqp,
        RabbitAmqpAdmin admin
    ) {
        super(jdbc, amqp, admin);
    }

    @Test
    void stampsTwice() throws Exception {
        final var message = message(coffeeSold(3));

        send(message);
        send(message);

        await().untilAsserted(() -> assertThat(cardTotal()).isEqualTo(6));
    }

}
