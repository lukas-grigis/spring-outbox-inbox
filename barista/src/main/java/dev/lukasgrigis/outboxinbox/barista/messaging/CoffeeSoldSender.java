package dev.lukasgrigis.outboxinbox.barista.messaging;

import dev.lukasgrigis.outboxinbox.barista.BaristaProperties;
import dev.lukasgrigis.outboxinbox.messaging.CoffeeSold;
import dev.lukasgrigis.outboxinbox.messaging.CoffeeSoldTopology;
import dev.lukasgrigis.outboxinbox.messaging.ContentHash;
import lombok.RequiredArgsConstructor;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbitmq.client.RabbitAmqpTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
public class CoffeeSoldSender {

    private final RabbitAmqpTemplate amqp;
    private final BaristaProperties properties;

    // runs after Jackson wrote the body, so the hash covers the bytes that go on the wire
    private static Message stamp(Message message, CoffeeSold coffeeSold) {
        final var properties = message.getMessageProperties();
        properties.setMessageId(coffeeSold.messageId().toString());
        properties.setHeader(ContentHash.HEADER, ContentHash.of(message.getBody()));
        return message;
    }

    // completes when the broker answers ACCEPTED; a rejected or released message, or no answer in time, fails it
    public CompletableFuture<Boolean> send(CoffeeSold coffeeSold) {
        return amqp
            .convertAndSend(
                CoffeeSoldTopology.EXCHANGE,
                CoffeeSoldTopology.ROUTING_KEY,
                coffeeSold,
                message -> stamp(message, coffeeSold)
            )
            // the template's own publishTimeout doesn't apply to this send
            .orTimeout(properties.sendTimeout().toMillis(), TimeUnit.MILLISECONDS);
    }

}
