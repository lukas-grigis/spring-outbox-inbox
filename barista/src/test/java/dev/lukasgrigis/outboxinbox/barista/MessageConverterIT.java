package dev.lukasgrigis.outboxinbox.barista;

import dev.lukasgrigis.outboxinbox.barista.service.SaleService;
import dev.lukasgrigis.outboxinbox.messaging.CoffeeSold;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbitmq.client.RabbitAmqpAdmin;
import org.springframework.amqp.rabbitmq.client.RabbitAmqpTemplate;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class MessageConverterIT extends AbstractBaristaIT {

    private final MessageConverter converter;

    @Autowired
    public MessageConverterIT(
        SaleService sales,
        TransactionTemplate transaction,
        JdbcTemplate jdbc,
        RabbitAmqpAdmin admin,
        RabbitAmqpTemplate amqp,
        Declarables topology,
        MessageConverter converter
    ) {
        super(sales, transaction, jdbc, admin, amqp, topology);
        this.converter = converter;
    }

    // the contract test pins the bytes of a default converter; this holds the application's converter to them
    @Test
    void writesTheBytesOfTheContractConverter() {
        final var coffeeSold = new CoffeeSold(UUID.randomUUID(), UUID.randomUUID(), "Café-42", 3, Instant.now());

        assertThat(converter.toMessage(coffeeSold, new MessageProperties()).getBody())
            .isEqualTo(new JacksonJsonMessageConverter().toMessage(coffeeSold, new MessageProperties()).getBody());
    }

}
