package dev.lukasgrigis.outboxinbox.loyalty.persistence;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.UUID;

public interface InboxRepository extends JpaRepository<InboxEntry, InboxEntry.Key> {

    // 1 the first time a message id is seen, 0 after that; a second instance inserting the same id at the same
    // moment waits for the first and gets 0. HQL, because JPQL has no INSERT; Hibernate hands ON CONFLICT DO NOTHING
    // to Postgres as written, and duplicates is left out so it takes the column default
    @Modifying
    @Query("""
        INSERT INTO InboxEntry (consumer, messageId, contentSha256) VALUES (:consumer, :messageId, :sha256)
        ON CONFLICT DO NOTHING
        """)
    int insertIfAbsent(
        @Param("consumer") String consumer,
        @Param("messageId") UUID messageId,
        @Param("sha256") String sha256
    );

    // 1 when the id was seen with the same content, a true duplicate; 0 when the content differs
    @Modifying
    @Query("""
        UPDATE InboxEntry e SET e.duplicates = e.duplicates + 1
        WHERE e.consumer = :consumer AND e.messageId = :messageId AND e.contentSha256 = :sha256
        """)
    int countDuplicate(
        @Param("consumer") String consumer,
        @Param("messageId") UUID messageId,
        @Param("sha256") String sha256
    );

}
