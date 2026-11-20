package dev.lukasgrigis.outboxinbox.loyalty;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("loyalty")
public record LoyaltyProperties(
    @DefaultValue("inbox") Mode mode,
    @DefaultValue("loyalty-1") String instanceId
) {

    public enum Mode {
        /**
         * Every delivery is stamped. A redelivery is stamped again.
         */
        NAIVE,
        /**
         * The first delivery of a message is stamped, later ones are recognised and only acknowledged.
         */
        INBOX
    }

}
