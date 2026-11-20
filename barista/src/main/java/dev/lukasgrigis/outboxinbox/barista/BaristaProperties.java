package dev.lukasgrigis.outboxinbox.barista;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

@ConfigurationProperties("barista")
public record BaristaProperties(
    @DefaultValue("outbox") Mode mode,
    @DefaultValue("barista-1") String instanceId,
    @DefaultValue("5s") Duration sendTimeout
) {

    public enum Mode {
        /**
         * Commit, then send. A crash in between loses the message.
         */
        AFTER_COMMIT,
        /**
         * Send inside the transaction. A rollback after the send leaves a ghost message.
         */
        IN_TRANSACTION,
        /**
         * Store the message with the sale; OutboxRelay sends it after the commit until the broker accepts it.
         */
        OUTBOX
    }

}
