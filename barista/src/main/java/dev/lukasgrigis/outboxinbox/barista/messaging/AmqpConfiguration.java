package dev.lukasgrigis.outboxinbox.barista.messaging;

import com.rabbitmq.client.amqp.BackOffDelayPolicy;
import com.rabbitmq.client.amqp.Environment;
import dev.lukasgrigis.outboxinbox.barista.BaristaProperties;
import org.springframework.amqp.rabbitmq.client.AmqpConnectionFactory;
import org.springframework.amqp.rabbitmq.client.SingleAmqpConnectionFactory;
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
    AmqpConnectionFactory amqpConnectionFactory(Environment environment, BaristaProperties properties) {
        return new SingleAmqpConnectionFactory(environment)
            .setConnectionName(properties.instanceId())
            .setRecovery(recovery -> recovery.backOffDelayPolicy(BackOffDelayPolicy.fixed(Duration.ofSeconds(1))))
            .setIdleTimeout(Duration.ofSeconds(5));
    }

}
