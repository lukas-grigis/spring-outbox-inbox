package dev.lukasgrigis.outboxinbox.loyalty;

import dev.lukasgrigis.outboxinbox.messaging.CoffeeSold;
import dev.lukasgrigis.outboxinbox.messaging.CoffeeSoldTopology;
import dev.lukasgrigis.outboxinbox.messaging.ContentHash;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbitmq.client.AmqpConnectionFactory;
import org.springframework.amqp.rabbitmq.client.RabbitAmqpAdmin;
import org.springframework.amqp.rabbitmq.client.RabbitAmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@ExtendWith(OutputCaptureExtension.class)
class ParkingIT extends AbstractLoyaltyIT {

    private final AmqpConnectionFactory connectionFactory;

    @Autowired
    public ParkingIT(
        JdbcTemplate jdbc,
        RabbitAmqpTemplate amqp,
        RabbitAmqpAdmin admin,
        AmqpConnectionFactory connectionFactory
    ) {
        super(jdbc, amqp, admin);
        this.connectionFactory = connectionFactory;
    }

    @Test
    void aBodyThatDoesNotMatchItsHashIsParked(CapturedOutput output) throws Exception {
        final var message = message(coffeeSold(1));
        message.getMessageProperties().setHeader(ContentHash.HEADER, "not the hash of this body");

        send(message);

        assertParkedWithoutEffect("rejected");
        assertThat(output).contains("parked: HASH_MISMATCH");
    }

    @Test
    void aMessageThatBreaksAConstraintIsParked(CapturedOutput output) throws Exception {
        send(message(coffeeSold(0)));

        assertParkedWithoutEffect("rejected");
        assertThat(output).contains("parked: INVALID");
    }

    @Test
    void anIdReusedForDifferentContentIsParkedAndTheOriginalStampStays(CapturedOutput output) throws Exception {
        final var original = coffeeSold(4);
        send(message(original));
        await().untilAsserted(() -> assertThat(cardTotal()).isEqualTo(4));

        send(message(new CoffeeSold(original.messageId(), original.saleId(), original.cardId(), 5, original.soldAt())));

        assertParkedWithoutEffect("rejected");
        assertThat(output).contains("parked: ID_REUSED");
        assertThat(cardTotal()).isEqualTo(4);
    }

    // the container cannot convert it, rejects it before the listener runs, and carries on
    @Test
    void aBodyThatIsNotJsonIsParkedAndTheNextMessageIsStillStamped() throws Exception {
        send(new Message("not json".getBytes()));

        assertParkedWithoutEffect("rejected");
        send(message(coffeeSold(2)));
        await().untilAsserted(() -> assertThat(cardTotal()).isEqualTo(2));
    }

    // no inbox row without its stamp: the only proof that both commit in one transaction
    @Test
    void aFailureThatKeepsHappeningParksAtTheDeliveryLimitAndLeavesNoInboxRow() throws Exception {
        jdbc.execute("ALTER TABLE stamp ADD CONSTRAINT poison CHECK (stamps <> 6)");
        try {
            send(message(coffeeSold(6)));

            assertParkedWithoutEffect("delivery_limit");
        } finally {
            jdbc.execute("ALTER TABLE stamp DROP CONSTRAINT poison");
        }
    }

    // the broker dead-lettered it for this reason, and no stamp or inbox row carries its content
    private void assertParkedWithoutEffect(String deathReason) throws Exception {
        final var parked = new CompletableFuture<com.rabbitmq.client.amqp.Message>();
        final var consumer = connectionFactory.getConnection()
            .consumerBuilder()
            .queue(CoffeeSoldTopology.PARKING_QUEUE)
            .messageHandler((context, message) -> {
                context.accept();
                parked.complete(message);
            })
            .build();
        final var message = parked.get(30, TimeUnit.SECONDS);
        consumer.close();

        final var sha256 = ContentHash.of(message.body());
        assertThat(message.annotation("x-last-death-reason")).isEqualTo(deathReason);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM stamp WHERE content_sha256 = ?", Integer.class, sha256))
            .isZero();
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM inbox WHERE content_sha256 = ?", Integer.class, sha256))
            .isZero();
    }

}
