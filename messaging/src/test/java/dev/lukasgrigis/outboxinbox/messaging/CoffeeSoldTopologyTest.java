package dev.lukasgrigis.outboxinbox.messaging;

import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Queue;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

class CoffeeSoldTopologyTest {

    // a queue argument cannot change on a live queue; a policy can
    @Test
    void queuesLeaveDeadLetteringAndTheDeliveryLimitToTheBrokerPolicy() {
        assertThat(new CoffeeSoldTopology().coffeeSoldDeclarables().getDeclarablesByType(Queue.class))
            .extracting(Queue::getName, Queue::getArguments)
            .containsExactlyInAnyOrder(
                tuple(CoffeeSoldTopology.QUEUE, Map.of("x-queue-type", "quorum")),
                tuple(CoffeeSoldTopology.PARKING_QUEUE, Map.of("x-queue-type", "quorum"))
            );
    }

}
