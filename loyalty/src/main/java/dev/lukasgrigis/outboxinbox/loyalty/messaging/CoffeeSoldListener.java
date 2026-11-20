package dev.lukasgrigis.outboxinbox.loyalty.messaging;

import com.rabbitmq.client.amqp.Consumer;
import com.rabbitmq.client.amqp.Message;
import dev.lukasgrigis.outboxinbox.loyalty.chaos.CrashPoints;
import dev.lukasgrigis.outboxinbox.loyalty.service.StampService;
import dev.lukasgrigis.outboxinbox.loyalty.service.Verdict;
import dev.lukasgrigis.outboxinbox.messaging.CoffeeSold;
import dev.lukasgrigis.outboxinbox.messaging.CoffeeSoldTopology;
import dev.lukasgrigis.outboxinbox.messaging.ContentHash;
import jakarta.validation.Validator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Map;

// settles by hand: the container releases a delivery that throws, and since RabbitMQ 4.3 a release no longer counts
// toward the delivery limit, so a message that always fails would loop forever
@Slf4j
@Component
@RequiredArgsConstructor
class CoffeeSoldListener {

    private final Validator validator;
    private final StampService stamps;
    private final CrashPoints crashPoints;

    @RabbitListener(queues = CoffeeSoldTopology.QUEUE, ackMode = "MANUAL")
    void onMessage(CoffeeSold coffeeSold, Message message, Consumer.Context context) {
        final Verdict verdict;
        try {
            verdict = verdict(coffeeSold, message);
        } catch (RuntimeException e) {
            log.warn(
                "Message {} failed on delivery {}, requeued: {}",
                coffeeSold.messageId(),
                message.deliveryCount(),
                e.getClass().getSimpleName()
            );
            // delivery-failed set: unlike a release, this requeue counts toward the delivery limit
            context.requeue(Map.of(), true);
            return;
        }
        if (verdict == Verdict.DUPLICATE) {
            log.info("Message {} duplicate on delivery {}", coffeeSold.messageId(), message.deliveryCount());
        }
        if (verdict.accepted()) {
            crashPoints.at(CrashPoints.AFTER_COMMIT_BEFORE_SETTLE, coffeeSold.messageId());
            context.accept();
        } else {
            log.warn("Message {} parked: {}", coffeeSold.messageId(), verdict);
            // rejected: the broker dead-letters it to the parking queue
            context.discard();
        }
    }

    private Verdict verdict(CoffeeSold coffeeSold, Message message) {
        final var sha256 = ContentHash.of(message.body());
        if (!sha256.equals(message.property(ContentHash.HEADER))) {
            return Verdict.HASH_MISMATCH;
        }
        if (!validator.validate(coffeeSold).isEmpty()) {
            return Verdict.INVALID;
        }
        return stamps.stamp(coffeeSold, new String(message.body(), StandardCharsets.UTF_8), sha256);
    }

}
