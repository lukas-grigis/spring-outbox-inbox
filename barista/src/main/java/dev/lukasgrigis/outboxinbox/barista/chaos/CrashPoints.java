package dev.lukasgrigis.outboxinbox.barista.chaos;

import dev.lukasgrigis.outboxinbox.barista.BaristaProperties;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.SplittableRandom;

// a crash is halt(137): no shutdown hooks, as with kill -9
@Component
public class CrashPoints {

    public static final String AFTER_COMMIT_BEFORE_SEND = "after-commit-before-send";
    public static final String AFTER_SEND_BEFORE_COMMIT = "after-send-before-commit";
    // between the broker's ACCEPTED and Modulith marking the publication complete
    public static final String AFTER_ACCEPT_BEFORE_COMPLETE = "after-accept-before-complete";

    private final String instanceId;
    private final ChaosProperties chaos;
    private final SplittableRandom random;

    CrashPoints(BaristaProperties properties, ChaosProperties chaos) {
        this.instanceId = properties.instanceId();
        this.chaos = chaos;
        // the restart count keeps a restarted instance from replaying the crashes of its previous life
        this.random = new SplittableRandom(Objects.hash(chaos.seed(), instanceId, chaos.restartCount()));
    }

    public void at(String point, Object saleId) {
        final var probability = chaos.crash().getOrDefault(point, 0.0);
        if (probability > 0 && roll() < probability) {
            // the one line the chaos run reads; flushed, because halt skips everything else
            System.err.println("halt(137) " + instanceId + " at " + point + ", sale " + saleId);
            System.err.flush();
            Runtime.getRuntime().halt(137);
        }
    }

    private synchronized double roll() {
        return random.nextDouble();
    }

}
