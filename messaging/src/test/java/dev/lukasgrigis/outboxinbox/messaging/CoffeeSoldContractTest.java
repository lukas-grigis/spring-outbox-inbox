package dev.lukasgrigis.outboxinbox.messaging;

import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;

import java.io.IOException;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class CoffeeSoldContractTest {

    // Changing this hash means changing the wire format: every producer and consumer must move together.
    private static final String FIXTURE_SHA256 = "658516bb7e0656db5f544fab53eaba0483bd2967e886e1e20cb3973ce83c15c4";

    private static final CoffeeSold FIXTURE = new CoffeeSold(
        UUID.fromString("0b9f2c1e-6d3a-4c58-9a47-3e1d5b7f8a20"),
        UUID.fromString("7c1e4a92-58d0-4b6f-a3e1-92f4c0d6b815"),
        "Café-42",
        3,
        Instant.parse("2026-01-15T10:30:00.123456Z")
    );

    private final JacksonJsonMessageConverter converter = new JacksonJsonMessageConverter();

    private static byte[] fixture() throws IOException {
        try (final var in = CoffeeSoldContractTest.class.getResourceAsStream("/contract/coffee-sold.json")) {
            return in.readAllBytes();
        }
    }

    // the listener container infers the target type from the listener parameter, so does this
    private CoffeeSold read(byte[] body) {
        final var properties = new MessageProperties();
        properties.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        properties.setInferredArgumentType(CoffeeSold.class);
        return (CoffeeSold) converter.fromMessage(new Message(body, properties));
    }

    @Test
    void encodesToTheGoldenFixtureBytes() throws IOException {
        final var message = converter.toMessage(FIXTURE, new MessageProperties());

        assertThat(message.getBody()).as("contract/coffee-sold.json is pinned byte for byte; an IDE reformat breaks it, restore the one-line file").isEqualTo(fixture());
        assertThat(message.getMessageProperties().getContentType()).isEqualTo(MessageProperties.CONTENT_TYPE_JSON);
    }

    @Test
    void goldenFixtureHashIsPinned() throws IOException {
        assertThat(ContentHash.of(fixture())).as("contract/coffee-sold.json is pinned byte for byte; an IDE reformat breaks it, restore the one-line file").isEqualTo(FIXTURE_SHA256);
    }

    @Test
    void decodesTheGoldenFixture() throws IOException {
        assertThat(read(fixture())).isEqualTo(FIXTURE);
    }

    @Test
    void hostileUnicodeSurvivesTheRoundTrip() {
        // quote, backslash, NUL, newline, U+2028, emoji (surrogate pair), combining mark, RTL override, BOM
        final var hostile = "\"\\\u0000\n 😀é‮﻿{}";
        final var message = new CoffeeSold(FIXTURE.messageId(), FIXTURE.saleId(), hostile, 1, FIXTURE.soldAt());

        assertThat(read(converter.toMessage(message, new MessageProperties()).getBody())).isEqualTo(message);
    }

}
