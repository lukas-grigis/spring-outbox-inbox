package dev.lukasgrigis.outboxinbox.barista.service;

import dev.lukasgrigis.outboxinbox.barista.BaristaProperties;
import dev.lukasgrigis.outboxinbox.barista.chaos.CrashPoints;
import dev.lukasgrigis.outboxinbox.barista.messaging.CoffeeSoldSender;
import dev.lukasgrigis.outboxinbox.barista.persistence.Sale;
import dev.lukasgrigis.outboxinbox.barista.persistence.SaleRepository;
import dev.lukasgrigis.outboxinbox.messaging.CoffeeSold;
import lombok.RequiredArgsConstructor;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

// one method, three modes, so each shows where its commit sits relative to the send
@Service
@RequiredArgsConstructor
public class SaleService {

    private final BaristaProperties properties;
    private final SaleRepository sales;
    private final TransactionTemplate transaction;
    private final CoffeeSoldSender sender;
    private final ApplicationEventPublisher events;
    private final CrashPoints crashPoints;

    private static CoffeeSold coffeeSold(Sale sale) {
        return new CoffeeSold(
            sale.getMessageId(),
            sale.getSaleId(),
            sale.getCardId(),
            sale.getCups(),
            sale.getSoldAt()
        );
    }

    public Receipt sell(String cardId, int cups) {
        final var sale = new Sale(cardId, cups);
        switch (properties.mode()) {
            case AFTER_COMMIT -> {
                sales.save(sale); // commits here: save runs in its own transaction
                crashPoints.at(CrashPoints.AFTER_COMMIT_BEFORE_SEND, sale.getSaleId());
                sender.send(coffeeSold(sale)).join();
            }
            case IN_TRANSACTION -> transaction.executeWithoutResult(status -> {
                // flushed now, so the insert reaches the database before the send
                sales.saveAndFlush(sale);
                sender.send(coffeeSold(sale)).join();
                crashPoints.at(CrashPoints.AFTER_SEND_BEFORE_COMMIT, sale.getSaleId());
            });
            // Modulith stores the event with the sale; OutboxRelay sends it after the commit
            case OUTBOX -> transaction.executeWithoutResult(status -> {
                sales.save(sale);
                events.publishEvent(coffeeSold(sale));
            });
        }
        return new Receipt(sale.getSaleId(), sale.getMessageId());
    }

}
