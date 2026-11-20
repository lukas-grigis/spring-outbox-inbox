package dev.lukasgrigis.outboxinbox.barista.messaging;

import dev.lukasgrigis.outboxinbox.barista.chaos.CrashPoints;
import dev.lukasgrigis.outboxinbox.messaging.CoffeeSold;
import lombok.RequiredArgsConstructor;
import org.springframework.modulith.events.ApplicationModuleListener;
import org.springframework.modulith.events.FailedEventPublications;
import org.springframework.modulith.events.ResubmissionOptions;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;

import java.util.concurrent.CompletableFuture;

// The relay of the outbox. Modulith stores each CoffeeSold in event_publication together with the sale, calls relay
// after the commit and marks the row completed only when the returned send completes. Modulith's own RabbitMQ
// externalizer speaks AMQP 0.9.1 only, hence this listener.
@Component
@RequiredArgsConstructor
class OutboxRelay {

    private final CoffeeSoldSender sender;
    private final FailedEventPublications failed;
    private final CrashPoints crashPoints;

    // sending needs no transaction; Spring allows only REQUIRES_NEW or NOT_SUPPORTED here
    @ApplicationModuleListener(propagation = Propagation.NOT_SUPPORTED)
    CompletableFuture<Void> relay(CoffeeSold coffeeSold) {
        return sender.send(coffeeSold)
            .thenRun(() -> crashPoints.at(CrashPoints.AFTER_ACCEPT_BEFORE_COMPLETE, coffeeSold.saleId()));
    }

    // a failed publication stays FAILED until someone resubmits it; so does one left half done by a crash, once it
    // is stale (spring.modulith.events.staleness)
    @Scheduled(fixedDelayString = "${barista.resubmit-interval}")
    void resubmit() {
        failed.resubmit(ResubmissionOptions.defaults());
    }

}
