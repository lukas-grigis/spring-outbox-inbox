--liquibase formatted sql

--changeset spring-outbox-inbox:1
CREATE TABLE sale
(
    sale_id    uuid PRIMARY KEY,
    message_id uuid        NOT NULL UNIQUE,
    card_id    text        NOT NULL,
    cups       int         NOT NULL,
    sold_at    timestamptz NOT NULL
);
