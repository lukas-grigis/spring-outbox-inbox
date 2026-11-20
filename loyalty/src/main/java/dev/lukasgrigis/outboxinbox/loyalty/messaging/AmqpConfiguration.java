package dev.lukasgrigis.outboxinbox.loyalty.messaging;

import com.rabbitmq.client.amqp.BackOffDelayPolicy;
import com.rabbitmq.client.amqp.Environment;
import dev.lukasgrigis.outboxinbox.loyalty.LoyaltyProperties;
import org.springframework.amqp.rabbit.config.ContainerCustomizer;
import org.springframework.amqp.rabbitmq.client.AmqpConnectionFactory;
import org.springframework.amqp.rabbitmq.client.SingleAmqpConnectionFactory;
import org.springframework.amqp.rabbitmq.client.listener.RabbitAmqpListenerContainer;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;

@Configuration
class AmqpConfiguration {

    @Bean
    MessageConverter messageConverter(JsonMapper jsonMapper) {
        return new JacksonJsonMessageConverter(jsonMapper);
    }

    // for the chaos run: a named connection, a 1 s reconnect, and a dead connection noticed after 5 s instead of 60 s
    @Bean
    AmqpConnectionFactory amqpConnectionFactory(Environment environment, LoyaltyProperties properties) {
        return new SingleAmqpConnectionFactory(environment)
            .setConnectionName(properties.instanceId())
            .setRecovery(recovery -> recovery.backOffDelayPolicy(BackOffDelayPolicy.fixed(Duration.ofSeconds(1))))
            .setIdleTimeout(Duration.ofSeconds(5));
    }

    // one message at a time: a dead connection counts every unsettled delivery toward the delivery limit
    @Bean
    ContainerCustomizer<RabbitAmqpListenerContainer> oneCredit() {
        return container -> container.setInitialCredits(1);
    }

}
