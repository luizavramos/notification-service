package br.com.motorfraude.notification.adapters.gateway.kafka;

import br.com.motorfraude.contracts.JsonCodec;
import br.com.motorfraude.notification.application.gateway.RetryScheduler;
import br.com.motorfraude.notification.domain.Delivery;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaRetryScheduler implements RetryScheduler {

    private static final long SEND_TIMEOUT_SECONDS = 10;

    private final Producer<String, String> producer;
    private final Clock clock;
    private final Map<Duration, String> topicByDelay;
    private final String dlqTopic;

    public KafkaRetryScheduler(Producer<String, String> producer, Clock clock, Map<Duration, String> topicByDelay, String dlqTopic) {
        this.producer = producer;
        this.clock = clock;
        this.topicByDelay = Map.copyOf(topicByDelay);
        this.dlqTopic = dlqTopic;
    }

    @Override
    public void scheduleRetry(Delivery delivery, Duration delay) {
        var topic = topicByDelay.get(delay);
        if (topic == null) {
            throw new IllegalArgumentException("nenhum tópico de retry configurado para o atraso " + delay);
        }
        var message = new RetryMessage(delivery.alertId(), delivery.channel(), delivery.attempts(), clock.instant().plus(delay));
        send(new ProducerRecord<>(topic, delivery.alertId(), JsonCodec.toJson(message)));
    }

    @Override
    public void sendToDeadLetter(Delivery delivery, String reason) {
        var body = new LinkedHashMap<String, Object>();
        body.put("alertId", delivery.alertId());
        body.put("channel", delivery.channel().name());
        body.put("attempts", delivery.attempts());
        body.put("reason", reason);
        body.put("at", clock.instant().toString());
        var record = new ProducerRecord<>(dlqTopic, delivery.alertId(), JsonCodec.toJson(body));
        record.headers().add("dlq.reason", reason.getBytes(StandardCharsets.UTF_8));
        send(record);
    }

    public void sendRawToDeadLetter(String key, byte[] value, String sourceTopic, int partition, long offset, String reason) {
        var record = new ProducerRecord<String, String>(dlqTopic, key, value == null ? null : new String(value, StandardCharsets.UTF_8));
        record.headers().add("dlq.reason", reason.getBytes(StandardCharsets.UTF_8));
        record.headers().add("dlq.source", (sourceTopic + "[" + partition + "]@" + offset).getBytes(StandardCharsets.UTF_8));
        send(record);
    }

    private void send(ProducerRecord<String, String> record) {
        try {
            producer.send(record).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("envio para " + record.topic() + " interrompido", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("não foi possível publicar em " + record.topic() + ": " + e.getMessage(), e);
        }
    }
}
