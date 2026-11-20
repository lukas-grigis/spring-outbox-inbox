package dev.lukasgrigis.outboxinbox.barista.web;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;

public record SaleRequest(@NotBlank String cardId, @Positive int cups) {

}
