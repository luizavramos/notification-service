package br.com.motorfraude.notification.adapters.controller.kafka;

import br.com.motorfraude.contracts.FraudAlert;
import br.com.motorfraude.contracts.JsonCodec;
import br.com.motorfraude.contracts.Schemas;
import br.com.motorfraude.notification.adapters.gateway.kafka.KafkaRetryScheduler;
import br.com.motorfraude.notification.application.usecase.RegisterAlertUseCase;
import br.com.motorfraude.notification.domain.Channel;
import br.com.motorfraude.notification.domain.Delivery;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AlertConsumerLoop {

    private static final Logger LOG = LoggerFactory.getLogger(AlertConsumerLoop.class);

    private final Consumer<String, String> consumer;
    private final String topic;
    private final RegisterAlertUseCase register;
    private final BiConsumer<String, Channel> dispatcher;
    private final Executor executor;
    private final KafkaRetryScheduler deadLetters;
    private final Clock clock;
    private final Duration failureBackoff;
    private final Map<TopicPartition, Instant> resumeAt = new HashMap<>();

    public AlertConsumerLoop(Consumer<String, String> consumer, String topic, RegisterAlertUseCase register, BiConsumer<String, Channel> dispatcher,
            Executor executor, KafkaRetryScheduler deadLetters, Clock clock, Duration failureBackoff) {
        this.consumer = consumer;
        this.topic = topic;
        this.register = register;
        this.dispatcher = dispatcher;
        this.executor = executor;
        this.deadLetters = deadLetters;
        this.clock = clock;
        this.failureBackoff = failureBackoff;
    }

    public void initialize() {
        consumer.subscribe(List.of(topic));
    }

    public void pollOnce(Duration timeout) {
        resumeDue();
        try {
            var records = consumer.poll(timeout);
            var toCommit = new HashMap<TopicPartition, OffsetAndMetadata>();
            var toDispatch = new ArrayList<Delivery>();
            for (var partition : records.partitions()) {
                for (var record : records.records(partition)) {
                    try {
                        handle(record, toDispatch);
                        toCommit.put(partition, new OffsetAndMetadata(record.offset() + 1));
                    } catch (RuntimeException e) {
                        LOG.warn("alerta de {}[{}]@{} não registrado; o offset não é confirmado e a leitura recua: {}", record.topic(), record.partition(),
                                record.offset(), e.toString());
                        consumer.seek(partition, record.offset());
                        consumer.pause(List.of(partition));
                        resumeAt.put(partition, clock.instant().plus(failureBackoff));
                        break;
                    }
                }
            }
            if (!toCommit.isEmpty()) {
                consumer.commitSync(toCommit);
            }
            toDispatch.forEach(delivery -> executor.execute(() -> dispatch(delivery)));
        } catch (WakeupException e) {
            throw e;
        } catch (RuntimeException e) {
            LOG.warn("falha ao ler {}: {}", topic, e.toString());
        }
    }

    private void handle(ConsumerRecord<String, String> record, List<Delivery> toDispatch) {
        FraudAlert alert;
        try {
            var tree = JsonCodec.readTree(record.value());
            var errors = Schemas.validate("fraud-alert.v1", tree).errors();
            if (!errors.isEmpty()) {
                throw new IllegalArgumentException("fora do contrato fraud-alert.v1: " + errors);
            }
            alert = JsonCodec.fromTree(tree, FraudAlert.class);
        } catch (RuntimeException e) {
            deadLetters.sendRawToDeadLetter(record.key(), record.value() == null ? null : record.value().getBytes(java.nio.charset.StandardCharsets.UTF_8),
                    record.topic(), record.partition(), record.offset(), String.valueOf(e.getMessage()));
            return;
        }
        toDispatch.addAll(register.register(alert).toDispatch());
    }

    private void dispatch(Delivery delivery) {
        try {
            dispatcher.accept(delivery.alertId(), delivery.channel());
        } catch (RuntimeException e) {
            LOG.warn("entrega {}:{} não despachada; fica PENDING para o reconciliador: {}", delivery.alertId(), delivery.channel(), e.toString());
        }
    }

    private void resumeDue() {
        var now = clock.instant();
        for (var entry : List.copyOf(resumeAt.entrySet())) {
            if (!entry.getValue().isAfter(now)) {
                resumeAt.remove(entry.getKey());
                consumer.resume(List.of(entry.getKey()));
            }
        }
    }
}
