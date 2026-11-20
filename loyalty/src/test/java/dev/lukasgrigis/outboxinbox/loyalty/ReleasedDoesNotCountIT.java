package dev.lukasgrigis.outboxinbox.loyalty;

import com.rabbitmq.client.amqp.Message;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.amqp.rabbitmq.client.RabbitAmqpAdmin;
import org.springframework.amqp.rabbitmq.client.RabbitAmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

// why loyalty settles by hand: the default turns a listener exception into a release, which is not a failed delivery
class ReleasedDoesNotCountIT extends AbstractLoyaltyIT {

    static final String QUEUE = "test.always-fails";
    static final int DELIVERY_LIMIT = 5;
    static final List<Long> DELIVERY_COUNTS = new CopyOnWriteArrayList<>();

    private final RabbitListenerEndpointRegistry registry;

    @Autowired
    public ReleasedDoesNotCountIT(
        JdbcTemplate jdbc,
        RabbitAmqpTemplate amqp,
        RabbitAmqpAdmin admin,
        RabbitListenerEndpointRegistry registry
    ) {
        super(jdbc, amqp, admin);
        this.registry = registry;
    }

    @Test
    void aListenerThatAlwaysThrowsIsRedeliveredPastTheLimitWithoutTheCountMoving() throws Exception {
        admin.declareQueue(QueueBuilder.durable(QUEUE).quorum().deliveryLimit(DELIVERY_LIMIT).build());
        try {
            // JSON without a type header, or the container fails converting it before the listener runs
            amqp.send(QUEUE, new org.springframework.amqp.core.Message("{}".getBytes())).get(10, TimeUnit.SECONDS);
            final var container = registry.getListenerContainer("always-fails");

            container.start();
            // a counted delivery would have dropped the message at the limit
            await().until(() -> DELIVERY_COUNTS.size() > 2 * DELIVERY_LIMIT);
            container.stop();

            assertThat(DELIVERY_COUNTS).containsOnly(0L);
        } finally {
            admin.deleteQueue(QUEUE);
        }
    }

    @TestConfiguration
    static class AlwaysFails {

        @RabbitListener(id = "always-fails", queues = QUEUE, autoStartup = "false")
        void onMessage(Message message) {
            DELIVERY_COUNTS.add(message.deliveryCount());
            throw new IllegalStateException("always fails");
        }

    }

}
