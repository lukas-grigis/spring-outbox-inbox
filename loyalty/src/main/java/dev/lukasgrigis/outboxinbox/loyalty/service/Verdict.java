package dev.lukasgrigis.outboxinbox.loyalty.service;

public enum Verdict {
    /**
     * Stamped.
     */
    APPLIED,
    /**
     * Seen before with the same content: nothing was done.
     */
    DUPLICATE,
    /**
     * Seen before with different content: nothing was done, and it is not a redelivery.
     */
    ID_REUSED,
    /**
     * The body does not match its content-sha256 header.
     */
    HASH_MISMATCH,
    /**
     * The body breaks a constraint on CoffeeSold.
     */
    INVALID;

    public boolean accepted() {
        return this == APPLIED || this == DUPLICATE;
    }
}
