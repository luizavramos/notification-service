package br.com.motorfraude.notification.infrastructure.config;

import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "notification")
public record NotificationProperties(Kafka kafka, Providers providers, Retry retry, Resilience resilience, Duration suppressionWindow,
        Reconciler reconciler) {

    public record Kafka(String bootstrapServers, String alertsTopic, String groupId, String dlqTopic, Duration failureBackoff) {
    }

    public record Providers(Antifraud antifraud, Endpoint push, Endpoint email, Endpoint sms, Endpoint contacts) {
    }

    public record Antifraud(String url, String secret, Duration timeout) {
    }

    public record Endpoint(String url, Duration timeout) {
    }

    public record Retry(List<Step> steps) {

        public record Step(Duration delay, String topic) {
        }
    }

    public record Resilience(int maxAttempts, Duration initialBackoff, double backoffMultiplier, double jitter, int slidingWindowSize,
            int minimumCalls, float failureRateThreshold, Duration openStateWait, int halfOpenCalls, int maxConcurrentCalls, Duration bulkheadMaxWait) {
    }

    public record Reconciler(Duration interval, Duration grace, int limit) {
    }
}
