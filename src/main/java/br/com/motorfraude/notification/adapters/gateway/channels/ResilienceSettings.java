package br.com.motorfraude.notification.adapters.gateway.channels;

import java.time.Duration;

public record ResilienceSettings(int maxAttempts, Duration initialBackoff, double backoffMultiplier, double jitter, int slidingWindowSize,
        int minimumCalls, float failureRateThreshold, Duration openStateWait, int halfOpenCalls, int maxConcurrentCalls,
        Duration bulkheadMaxWait) {

    public ResilienceSettings {
        if (maxAttempts < 1 || slidingWindowSize < 1 || minimumCalls < 1 || halfOpenCalls < 1 || maxConcurrentCalls < 1) {
            throw new IllegalArgumentException("as quantidades da resiliência devem ser 1 ou mais");
        }
        if (jitter < 0 || jitter >= 1) {
            throw new IllegalArgumentException("o jitter deve estar em [0, 1)");
        }
    }

    public static ResilienceSettings defaults() {
        return new ResilienceSettings(3, Duration.ofMillis(200), 2.0, 0.5, 20, 10, 50f, Duration.ofSeconds(30), 3, 50, Duration.ofMillis(100));
    }
}
