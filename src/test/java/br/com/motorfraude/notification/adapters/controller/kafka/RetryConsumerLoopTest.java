package br.com.motorfraude.notification.adapters.controller.kafka;

import static br.com.motorfraude.notification.TestAlerts.NOW;
import static org.assertj.core.api.Assertions.assertThat;

import br.com.motorfraude.contracts.JsonCodec;
import br.com.motorfraude.notification.adapters.gateway.kafka.RetryMessage;
import br.com.motorfraude.notification.domain.Channel;
import br.com.motorfraude.notification.fakes.MutableClock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RetryConsumerLoopTest {

    private static final String TOPIC = "alerts.retry.1m";
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);

    private final MockConsumer<String, String> consumer = new MockConsumer<>("earliest");
    private final MutableClock clock = new MutableClock(NOW);
    private final List<String> dispatched = new ArrayList<>();
    private boolean failNext;
    private RetryConsumerLoop loop;
    private long offset;

    @BeforeEach
    void setUp() {
        loop = new RetryConsumerLoop(consumer, TOPIC, (alertId, channel) -> {
            if (failNext) {
                failNext = false;
                throw new IllegalStateException("banco fora do ar");
            }
            dispatched.add(alertId + ":" + channel);
        }, clock, Duration.ofSeconds(5));
        loop.initialize();
        consumer.rebalance(List.of(PARTITION));
        consumer.updateBeginningOffsets(java.util.Map.of(PARTITION, 0L));
    }

    private void publish(String alertId, Channel channel, Instant dueAt) {
        var message = JsonCodec.toJson(new RetryMessage(alertId, channel, 1, dueAt));
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, offset++, alertId, message));
    }

    private long committed() {
        var committed = consumer.committed(Set.of(PARTITION)).get(PARTITION);
        return committed == null ? -1 : committed.offset();
    }

    @Test
    @DisplayName("mensagem já vencida é despachada e o offset confirmado")
    void vencida() {
        publish("a1", Channel.PUSH, NOW.minusSeconds(1));

        loop.pollOnce(Duration.ZERO);

        assertThat(dispatched).containsExactly("a1:PUSH");
        assertThat(committed()).isEqualTo(1);
    }

    @Test
    @DisplayName("mensagem que ainda não venceu pausa só aquela partição, não despacha e não confirma; vencida, despacha e retoma")
    void aindaNaoVenceu() {
        publish("a1", Channel.PUSH, NOW.plusSeconds(60));

        loop.pollOnce(Duration.ZERO);

        assertThat(dispatched).isEmpty();
        assertThat(consumer.paused()).containsExactly(PARTITION);
        assertThat(committed()).isEqualTo(-1);

        clock.advance(Duration.ofSeconds(30));
        loop.pollOnce(Duration.ZERO);
        assertThat(dispatched).isEmpty();
        assertThat(consumer.paused()).containsExactly(PARTITION);

        clock.advance(Duration.ofSeconds(31));
        loop.pollOnce(Duration.ZERO);

        assertThat(dispatched).containsExactly("a1:PUSH");
        assertThat(consumer.paused()).isEmpty();
        assertThat(committed()).isEqualTo(1);
    }

    @Test
    @DisplayName("a ordem é preservada: quem vem depois de uma mensagem não vencida espera por ela")
    void ordemPreservada() {
        publish("a1", Channel.PUSH, NOW.plusSeconds(60));
        publish("a2", Channel.EMAIL, NOW.minusSeconds(5));

        loop.pollOnce(Duration.ZERO);
        assertThat(dispatched).isEmpty();

        clock.advance(Duration.ofSeconds(61));
        loop.pollOnce(Duration.ZERO);

        assertThat(dispatched).containsExactly("a1:PUSH", "a2:EMAIL");
        assertThat(committed()).isEqualTo(2);
    }

    @Test
    @DisplayName("falha ao despachar (ex.: banco fora do ar): não confirma, espera o intervalo e tenta a mesma mensagem de novo")
    void falhaTentaDeNovo() {
        publish("a1", Channel.PUSH, NOW.minusSeconds(1));
        failNext = true;

        loop.pollOnce(Duration.ZERO);

        assertThat(dispatched).isEmpty();
        assertThat(committed()).isEqualTo(-1);
        assertThat(consumer.paused()).containsExactly(PARTITION);

        clock.advance(Duration.ofSeconds(6));
        loop.pollOnce(Duration.ZERO);

        assertThat(dispatched).containsExactly("a1:PUSH");
        assertThat(committed()).isEqualTo(1);
    }

    @Test
    @DisplayName("mensagem ilegível é descartada com o offset confirmado, sem travar a fila")
    void ilegivel() {
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, offset++, "x", "{não é json"));
        publish("a2", Channel.EMAIL, NOW.minusSeconds(1));

        loop.pollOnce(Duration.ZERO);

        assertThat(dispatched).containsExactly("a2:EMAIL");
        assertThat(committed()).isEqualTo(2);
    }

    @Test
    @DisplayName("ao perder a partição (rebalance), a mensagem em espera é solta: o novo dono a lê do offset não confirmado")
    void rebalanceSolta() {
        publish("a1", Channel.PUSH, NOW.plusSeconds(60));
        loop.pollOnce(Duration.ZERO);

        consumer.rebalance(List.of());
        clock.advance(Duration.ofSeconds(120));
        loop.pollOnce(Duration.ZERO);

        assertThat(dispatched).isEmpty();
        assertThat(loop.heldMessages()).isZero();
    }
}
