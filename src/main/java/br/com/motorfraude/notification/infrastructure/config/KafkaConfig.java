package br.com.motorfraude.notification.infrastructure.config;

import br.com.motorfraude.notification.adapters.controller.kafka.AlertConsumerLoop;
import br.com.motorfraude.notification.adapters.controller.kafka.ConsumerLoopRunner;
import br.com.motorfraude.notification.adapters.controller.kafka.RetryConsumerLoop;
import br.com.motorfraude.notification.adapters.gateway.kafka.KafkaRetryScheduler;
import br.com.motorfraude.notification.application.gateway.DeliveryMetrics;
import br.com.motorfraude.notification.application.usecase.DeliveryDispatcher;
import br.com.motorfraude.notification.application.usecase.ReconcilePendingDeliveriesUseCase;
import br.com.motorfraude.notification.application.usecase.RegisterAlertUseCase;
import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {

    @Bean(destroyMethod = "close")
    KafkaProducer<String, String> notificationProducer(NotificationProperties properties) {
        return new KafkaProducer<>(Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.kafka().bootstrapServers(),
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
                ProducerConfig.CLIENT_ID_CONFIG, "notification-service",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class));
    }

    @Bean
    KafkaRetryScheduler kafkaRetryScheduler(KafkaProducer<String, String> producer, Clock clock, NotificationProperties properties) {
        var topics = new LinkedHashMap<java.time.Duration, String>();
        properties.retry().steps().forEach(step -> topics.put(step.delay(), step.topic()));
        return new KafkaRetryScheduler(producer, clock, topics, properties.kafka().dlqTopic());
    }

    @Bean
    ConsumersLifecycle consumersLifecycle(NotificationProperties properties, RegisterAlertUseCase register, DeliveryDispatcher dispatcher,
            ReconcilePendingDeliveriesUseCase reconciler, KafkaRetryScheduler deadLetters, Clock clock, DeliveryMetrics metrics) {
        var kafka = properties.kafka();
        var runners = new java.util.ArrayList<ConsumerLoopRunner>();

        var alerts = new KafkaConsumer<String, String>(consumerConfig(kafka, kafka.groupId(), "notification-alerts"));
        var alertLoop = new AlertConsumerLoop(alerts, kafka.alertsTopic(), register, dispatcher::dispatch, new BoundedExecutor(Executors.newVirtualThreadPerTaskExecutor(), properties.resilience().maxConcurrentCalls()),
                deadLetters, clock, kafka.failureBackoff(), metrics);
        runners.add(new ConsumerLoopRunner("alert-consumer", alerts, alertLoop::initialize, alertLoop::pollOnce));

        for (var step : properties.retry().steps()) {
            var group = kafka.groupId() + "-" + step.topic();
            var consumer = new KafkaConsumer<String, String>(consumerConfig(kafka, group, "notification-" + step.topic()));
            var loop = new RetryConsumerLoop(consumer, step.topic(), dispatcher::dispatch, clock, kafka.failureBackoff());
            runners.add(new ConsumerLoopRunner("retry-consumer-" + step.topic(), consumer, loop::initialize, loop::pollOnce));
        }
        return new ConsumersLifecycle(runners, reconciler, properties.reconciler().interval());
    }

    @Bean
    HealthIndicator alertConsumersHealthIndicator(ConsumersLifecycle consumers) {
        return () -> consumers.allRunning() ? Health.up().build() : Health.down().withDetail("motivo", "algum laço de consumo parou").build();
    }

    private static Map<String, Object> consumerConfig(NotificationProperties.Kafka kafka, String groupId, String clientId) {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, groupId,
                ConsumerConfig.CLIENT_ID_CONFIG, clientId,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    }
}
