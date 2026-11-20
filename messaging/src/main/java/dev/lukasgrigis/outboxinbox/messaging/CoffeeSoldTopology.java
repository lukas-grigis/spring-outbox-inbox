package dev.lukasgrigis.outboxinbox.messaging;

import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

// dead-letter target, delivery limit and overflow are a broker policy (support/rabbitmq/definitions.json)
@Configuration
public class CoffeeSoldTopology {

    public static final String EXCHANGE = "barista";
    public static final String QUEUE = "loyalty.coffee-sold";
    public static final String ROUTING_KEY = "coffee.sold";
    public static final String PARKING_QUEUE = QUEUE + ".failed";

    @Bean
    Declarables coffeeSoldDeclarables() {
        final var exchange = new TopicExchange(EXCHANGE);
        final var queue = QueueBuilder.durable(QUEUE).quorum().build();
        final var parking = QueueBuilder.durable(PARKING_QUEUE).quorum().build();
        return new Declarables(exchange, queue, parking, BindingBuilder.bind(queue).to(exchange).with(ROUTING_KEY));
    }

}
