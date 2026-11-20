package dev.lukasgrigis.outboxinbox.loyalty.chaos;

import dev.lukasgrigis.outboxinbox.loyalty.LoyaltyProperties;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.SplittableRandom;

// a crash is halt(137): no shutdown hooks, as with kill -9
@Component
public class CrashPoints {

    // the stamp is committed, the broker has not been told
    public static final String AFTER_COMMIT_BEFORE_SETTLE = "after-commit-before-settle";

    private final String instanceId;
    private final ChaosProperties chaos;
    private final SplittableRandom random;

    CrashPoints(LoyaltyProperties properties, ChaosProperties chaos) {
        this.instanceId = properties.instanceId();
        this.chaos = chaos;
        // the restart count keeps a restarted instance from replaying the crashes of its previous life
        this.random = new SplittableRandom(Objects.hash(chaos.seed(), instanceId, chaos.restartCount()));
    }

    public void at(String point, Object messageId) {
        final var probability = chaos.crash().getOrDefault(point, 0.0);
        if (probability > 0 && roll() < probability) {
            // the one line the chaos run reads; flushed, because halt skips everything else
            System.err.println("halt(137) " + instanceId + " at " + point + ", message " + messageId);
            System.err.flush();
            Runtime.getRuntime().halt(137);
        }
    }

    private synchronized double roll() {
        return random.nextDouble();
    }

}
