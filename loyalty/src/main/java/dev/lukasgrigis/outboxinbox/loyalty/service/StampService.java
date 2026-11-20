package dev.lukasgrigis.outboxinbox.loyalty.service;

import dev.lukasgrigis.outboxinbox.loyalty.LoyaltyProperties;
import dev.lukasgrigis.outboxinbox.loyalty.persistence.InboxRepository;
import dev.lukasgrigis.outboxinbox.loyalty.persistence.Stamp;
import dev.lukasgrigis.outboxinbox.loyalty.persistence.StampRepository;
import dev.lukasgrigis.outboxinbox.messaging.CoffeeSold;
import dev.lukasgrigis.outboxinbox.messaging.CoffeeSoldTopology;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class StampService {

    private final LoyaltyProperties properties;
    private final InboxRepository inbox;
    private final StampRepository stamps;

    // the inbox row and the stamp commit together, and this returns after the commit, so the listener settles with
    // the broker knowing the outcome is durable
    @Transactional
    public Verdict stamp(CoffeeSold coffeeSold, String body, String sha256) {
        if (seenBefore(coffeeSold.messageId(), sha256)) {
            return repeatVerdict(coffeeSold.messageId(), sha256);
        }
        stamps.save(new Stamp(
            coffeeSold.messageId(),
            coffeeSold.saleId(),
            coffeeSold.cardId(),
            coffeeSold.cups(),
            body,
            sha256
        ));
        return Verdict.APPLIED;
    }

    // the inbox: records the message id, so a second delivery finds it; NAIVE skips it and stamps every delivery
    private boolean seenBefore(UUID messageId, String sha256) {
        return properties.mode() == LoyaltyProperties.Mode.INBOX
            && inbox.insertIfAbsent(CoffeeSoldTopology.QUEUE, messageId, sha256) == 0;
    }

    // same id with the same content is a redelivery; same id with other content is a bug at the producer
    private Verdict repeatVerdict(UUID messageId, String sha256) {
        return inbox.countDuplicate(CoffeeSoldTopology.QUEUE, messageId, sha256) == 1
            ? Verdict.DUPLICATE
            : Verdict.ID_REUSED;
    }

}
