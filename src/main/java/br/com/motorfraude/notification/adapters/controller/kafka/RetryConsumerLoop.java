package br.com.motorfraude.notification.adapters.controller.kafka;

import br.com.motorfraude.contracts.JsonCodec;
import br.com.motorfraude.notification.adapters.gateway.kafka.RetryMessage;
import br.com.motorfraude.notification.domain.Channel;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RetryConsumerLoop {

    private static final Logger LOG = LoggerFactory.getLogger(RetryConsumerLoop.class);

    private final Consumer<String, String> consumer;
    private final String topic;
    private final BiConsumer<String, Channel> dispatcher;
    private final Clock clock;
    private final Duration failureBackoff;
    private final Map<TopicPartition, Deque<ConsumerRecord<String, String>>> held = new HashMap<>();
    private final Map<TopicPartition, Instant> resumeAt = new HashMap<>();

    public RetryConsumerLoop(Consumer<String, String> consumer, String topic, BiConsumer<String, Channel> dispatcher, Clock clock,
            Duration failureBackoff) {
        this.consumer = consumer;
        this.topic = topic;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.failureBackoff = failureBackoff;
    }

    public void initialize() {
        consumer.subscribe(List.of(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                partitions.forEach(partition -> {
                    held.remove(partition);
                    resumeAt.remove(partition);
                });
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            }
        });
    }

    public int heldMessages() {
        return held.values().stream().mapToInt(Deque::size).sum();
    }

    public void pollOnce(Duration timeout) {
        resumeDue();
        try {
            var records = consumer.poll(timeout);
            for (var partition : records.partitions()) {
                var batch = new ArrayDeque<>(records.records(partition));
                held.computeIfAbsent(partition, p -> new ArrayDeque<>()).addAll(batch);
                if (!resumeAt.containsKey(partition)) {
                    drain(partition);
                }
            }
        } catch (WakeupException e) {
            throw e;
        } catch (RuntimeException e) {
            LOG.warn("falha ao ler {}: {}", topic, e.toString());
        }
    }

    private void resumeDue() {
        var now = clock.instant();
        for (var entry : List.copyOf(resumeAt.entrySet())) {
            if (!entry.getValue().isAfter(now)) {
                resumeAt.remove(entry.getKey());
                drain(entry.getKey());
            }
        }
    }

    private void drain(TopicPartition partition) {
        var queue = held.get(partition);
        long committedUpTo = -1;
        while (queue != null && !queue.isEmpty()) {
            var record = queue.peekFirst();
            RetryMessage message = parse(record);
            if (message != null) {
                var now = clock.instant();
                if (message.dueAt().isAfter(now)) {
                    wait(partition, message.dueAt());
                    break;
                }
                try {
                    dispatcher.accept(message.alertId(), message.channel());
                } catch (RuntimeException e) {
                    LOG.warn("retry de {}:{} não despachado, tenta de novo em {}: {}", message.alertId(), message.channel(), failureBackoff, e.toString());
                    wait(partition, clock.instant().plus(failureBackoff));
                    break;
                }
            }
            queue.pollFirst();
            committedUpTo = record.offset();
        }
        if (queue != null && queue.isEmpty() && !resumeAt.containsKey(partition)) {
            held.remove(partition);
            if (consumer.paused().contains(partition)) {
                consumer.resume(List.of(partition));
            }
        }
        if (committedUpTo >= 0) {
            consumer.commitSync(Map.of(partition, new OffsetAndMetadata(committedUpTo + 1)));
        }
    }

    private void wait(TopicPartition partition, Instant until) {
        resumeAt.put(partition, until);
        consumer.pause(List.of(partition));
    }

    private RetryMessage parse(ConsumerRecord<String, String> record) {
        try {
            var message = JsonCodec.fromJson(record.value(), RetryMessage.class);
            if (message.alertId() == null || message.channel() == null || message.dueAt() == null) {
                throw new IllegalArgumentException("mensagem de retry incompleta");
            }
            return message;
        } catch (RuntimeException e) {
            LOG.error("mensagem de retry ilegível descartada em {}[{}]@{}: {}", record.topic(), record.partition(), record.offset(), e.toString());
            return null;
        }
    }
}
