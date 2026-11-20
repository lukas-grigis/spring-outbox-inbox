package dev.lukasgrigis.outboxinbox.loyalty;

import dev.lukasgrigis.outboxinbox.messaging.CoffeeSoldTopology;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@ConfigurationPropertiesScan
@Import(CoffeeSoldTopology.class)
public class LoyaltyApplication {

    static void main(String[] args) {
        SpringApplication.run(LoyaltyApplication.class, args);
    }

}
