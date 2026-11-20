package dev.lukasgrigis.outboxinbox.barista;

import dev.lukasgrigis.outboxinbox.messaging.CoffeeSoldTopology;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
@Import(CoffeeSoldTopology.class)
public class BaristaApplication {

    static void main(String[] args) {
        SpringApplication.run(BaristaApplication.class, args);
    }

}
