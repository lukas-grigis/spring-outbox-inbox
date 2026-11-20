package dev.lukasgrigis.outboxinbox.barista.service;

import java.util.UUID;

public record Receipt(UUID saleId, UUID messageId) {

}
