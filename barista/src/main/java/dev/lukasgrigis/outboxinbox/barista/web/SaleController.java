package dev.lukasgrigis.outboxinbox.barista.web;

import dev.lukasgrigis.outboxinbox.barista.service.Receipt;
import dev.lukasgrigis.outboxinbox.barista.service.SaleService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/sales")
@RequiredArgsConstructor
class SaleController {

    private final SaleService sales;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    Receipt sell(@Valid @RequestBody SaleRequest request) {
        return sales.sell(request.cardId(), request.cups());
    }

}
