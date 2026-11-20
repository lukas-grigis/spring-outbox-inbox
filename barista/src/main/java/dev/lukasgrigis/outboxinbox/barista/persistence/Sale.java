package dev.lukasgrigis.outboxinbox.barista.persistence;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Entity
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Sale {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID saleId;
    private UUID messageId;
    private String cardId;
    private int cups;
    private Instant soldAt;

    public Sale(String cardId, int cups) {
        this.messageId = UUID.randomUUID();
        this.cardId = cardId;
        this.cups = cups;
        this.soldAt = Instant.now();
    }

}
