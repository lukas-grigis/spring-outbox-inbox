package dev.lukasgrigis.outboxinbox.loyalty.chaos;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.util.Map;

// set by the chaos run; without it no crash point ever fires
@ConfigurationProperties("chaos")
public record ChaosProperties(
    @DefaultValue("0") long seed,
    @DefaultValue("0") int restartCount,
    @DefaultValue Map<String, Double> crash
) {

}
