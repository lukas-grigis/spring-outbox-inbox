package dev.lukasgrigis.outboxinbox.barista;

import dev.lukasgrigis.outboxinbox.barista.service.SaleService;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.rabbit.core.AmqpNackReceivedException;
import org.springframework.amqp.rabbitmq.client.RabbitAmqpAdmin;
import org.springframework.amqp.rabbitmq.client.RabbitAmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@SpringBootTest(properties = "barista.mode=after-commit")
class BrokerRefusesAfterCommitIT extends AbstractBaristaIT {

    @Autowired
    public BrokerRefusesAfterCommitIT(
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
    void losesTheMessageAndNothingResendsIt() {
        refuseMessages();
        assertThatThrownBy(() -> sales.sell("card-1", 3)).hasRootCauseInstanceOf(AmqpNackReceivedException.class);
        assertThat(count("sale")).isOne();
        assertThat(count("event_publication")).isZero();

        acceptMessages();
        // longer than barista.resubmit-interval
        await().during(Duration.ofSeconds(6))
            .atMost(Duration.ofSeconds(10))
            .untilAsserted(() -> assertThat(queueDepth()).isZero());
    }

}
