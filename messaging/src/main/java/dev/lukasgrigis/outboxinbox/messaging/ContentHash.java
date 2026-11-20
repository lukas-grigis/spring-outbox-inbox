package dev.lukasgrigis.outboxinbox.messaging;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

// SHA-256 of the exact body bytes in lower-case hex, the same string Postgres gives for encode(sha256(body), 'hex')
public final class ContentHash {

    public static final String HEADER = "content-sha256";

    private ContentHash() {
    }

    public static String of(byte[] body) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(body));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

}
