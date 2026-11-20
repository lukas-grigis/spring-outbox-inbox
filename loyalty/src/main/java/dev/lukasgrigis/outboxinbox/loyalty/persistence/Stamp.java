package dev.lukasgrigis.outboxinbox.loyalty.persistence;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Entity
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Stamp {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long stampId;
    private UUID messageId;
    private UUID saleId;
    private String cardId;
    private int stamps;
    // the exact message text and the hash this JVM computed on receipt
    private String body;
    private String contentSha256;

    public Stamp(
        UUID messageId,
        UUID saleId,
        String cardId,
        int stamps,
        String body,
        String contentSha256
    ) {
        this.messageId = messageId;
        this.saleId = saleId;
        this.cardId = cardId;
        this.stamps = stamps;
        this.body = body;
        this.contentSha256 = contentSha256;
    }

}
