package br.com.motorfraude.notification.adapters.gateway.kafka;

import static br.com.motorfraude.notification.TestAlerts.NOW;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import br.com.motorfraude.contracts.JsonCodec;
import br.com.motorfraude.notification.domain.Channel;
import br.com.motorfraude.notification.domain.Delivery;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class KafkaRetrySchedulerTest {

    private final MockProducer<String, String> producer = new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());

    private KafkaRetryScheduler scheduler(MockProducer<String, String> p) {
        var topics = new LinkedHashMap<Duration, String>();
        topics.put(Duration.ofMinutes(1), "alerts.retry.1m");
        topics.put(Duration.ofMinutes(5), "alerts.retry.5m");
        topics.put(Duration.ofMinutes(30), "alerts.retry.30m");
        return new KafkaRetryScheduler(p, Clock.fixed(NOW, ZoneOffset.UTC), topics, "alerts.dlq");
    }

    private static Delivery delivery(int attempts) {
        var pending = Delivery.pending("alert-1", Channel.PUSH, NOW);
        for (int i = 0; i < attempts; i++) {
            pending = pending.retryScheduled("503", NOW);
        }
        return pending;
    }

    @Test
    @DisplayName("FR-004: o retry vai para o tópico do atraso, com a chave alertId e a mensagem {alertId, canal, tentativa, dueAt}")
    void agendaNoTopicoDoAtraso() {
        var s = scheduler(producer);

        s.scheduleRetry(delivery(1), Duration.ofMinutes(1));
        s.scheduleRetry(delivery(2), Duration.ofMinutes(5));
        s.scheduleRetry(delivery(3), Duration.ofMinutes(30));

        assertThat(producer.history()).extracting(ProducerRecord::topic).containsExactly("alerts.retry.1m", "alerts.retry.5m", "alerts.retry.30m");
        var first = producer.history().getFirst();
        assertThat(first.key()).isEqualTo("alert-1");
        var message = JsonCodec.fromJson(first.value(), RetryMessage.class);
        assertThat(message).isEqualTo(new RetryMessage("alert-1", Channel.PUSH, 1, NOW.plus(Duration.ofMinutes(1))));
    }

    @Test
    @DisplayName("atraso sem tópico configurado é erro de configuração")
    void atrasoDesconhecido() {
        assertThatThrownBy(() -> scheduler(producer).scheduleRetry(delivery(1), Duration.ofMinutes(2))).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("PT2M");
    }

    @Test
    @DisplayName("a DLQ recebe a entrega, o motivo e a hora, com a chave alertId")
    void dlq() {
        scheduler(producer).sendToDeadLetter(delivery(4), "HTTP 503");

        var record = producer.history().getFirst();
        assertThat(record.topic()).isEqualTo("alerts.dlq");
        assertThat(record.key()).isEqualTo("alert-1");
        var body = JsonCodec.readTree(record.value());
        assertThat(body.get("alertId").asString()).isEqualTo("alert-1");
        assertThat(body.get("channel").asString()).isEqualTo("PUSH");
        assertThat(body.get("reason").asString()).isEqualTo("HTTP 503");
        assertThat(body.get("attempts").asInt()).isEqualTo(4);
    }

    @Test
    @DisplayName("mensagem de alerta que não se lê vai bruta para a DLQ, com o motivo e a origem nos cabeçalhos")
    void dlqBruta() {
        scheduler(producer).sendRawToDeadLetter("acc_tok_1", "{ruim".getBytes(StandardCharsets.UTF_8), "fraud-alerts.v1", 2, 41L, "JSON inválido");

        var record = producer.history().getFirst();
        assertThat(record.topic()).isEqualTo("alerts.dlq");
        assertThat(record.value()).isEqualTo("{ruim");
        assertThat(new String(record.headers().lastHeader("dlq.reason").value(), StandardCharsets.UTF_8)).isEqualTo("JSON inválido");
        assertThat(new String(record.headers().lastHeader("dlq.source").value(), StandardCharsets.UTF_8)).isEqualTo("fraud-alerts.v1[2]@41");
    }

    @Test
    @DisplayName("se o Kafka recusar a mensagem, o erro sobe: a entrega fica PENDING e o reconciliador a recupera")
    void falhaSobe() {
        var broken = new MockProducer<String, String>(true, null, new StringSerializer(), new StringSerializer()) {
            @Override
            public synchronized Future<RecordMetadata> send(ProducerRecord<String, String> record, Callback callback) {
                throw new KafkaException("broker fora do ar");
            }
        };

        assertThatThrownBy(() -> scheduler(broken).scheduleRetry(delivery(1), Duration.ofMinutes(1))).isInstanceOf(RuntimeException.class);
    }
}
