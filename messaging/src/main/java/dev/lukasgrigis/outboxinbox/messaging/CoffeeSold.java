package dev.lukasgrigis.outboxinbox.messaging;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.time.Instant;
import java.util.UUID;

public record CoffeeSold(
    @NotNull UUID messageId,
    @NotNull UUID saleId,
    @NotBlank String cardId,
    @Positive int cups,
    @NotNull Instant soldAt
) {

}
