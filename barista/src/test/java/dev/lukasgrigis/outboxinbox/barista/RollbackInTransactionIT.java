package dev.lukasgrigis.outboxinbox.barista;

import dev.lukasgrigis.outboxinbox.barista.service.SaleService;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.rabbitmq.client.RabbitAmqpAdmin;
import org.springframework.amqp.rabbitmq.client.RabbitAmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = "barista.mode=in-transaction")
class RollbackInTransactionIT extends AbstractBaristaIT {

    @Autowired
    public RollbackInTransactionIT(
        SaleService sales,
        TransactionTemplate transaction,
        JdbcTemplate jdbc,
        RabbitAmqpAdmin admin,
        RabbitAmqpTemplate amqp,
        Declarables topology
    ) {
        super(sales, transaction, jdbc, admin, amqp, topology);
    }

    // sell() joins the surrounding transaction, which rolls back
    @Test
    void leavesAGhostMessageWithoutASale() throws Exception {
        final var receipt = transaction.execute(status -> {
            status.setRollbackOnly();
            return sales.sell("card-1", 3);
        });

        assertThat(count("sale")).isZero();
        assertThat(receive().getMessageProperties().getMessageId()).isEqualTo(receipt.messageId().toString());
    }

}
