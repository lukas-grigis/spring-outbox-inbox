package dev.lukasgrigis.outboxinbox.loyalty.persistence;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.UUID;

@Entity
@Table(name = "inbox")
@IdClass(InboxEntry.Key.class)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
class InboxEntry {

    @Id
    private String consumer;
    @Id
    private UUID messageId;
    private String contentSha256;
    private int duplicates;

    record Key(String consumer, UUID messageId) implements Serializable {

    }

}
