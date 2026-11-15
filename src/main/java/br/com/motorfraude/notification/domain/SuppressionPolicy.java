package br.com.motorfraude.notification.domain;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

public record SuppressionPolicy(Duration window) {

    public static final Duration DEFAULT_WINDOW = Duration.ofMinutes(10);

    public SuppressionPolicy {
        if (window == null || window.isZero() || window.isNegative()) {
            throw new IllegalArgumentException("a janela de supressão deve ser positiva");
        }
    }

    public static SuppressionPolicy defaultPolicy() {
        return new SuppressionPolicy(DEFAULT_WINDOW);
    }

    public boolean shouldSuppress(Channel channel, Optional<Instant> previousHighAlertAt, Instant detectedAt) {
        if (!channel.customerFacing() || previousHighAlertAt.isEmpty()) {
            return false;
        }
        return Duration.between(previousHighAlertAt.get(), detectedAt).abs().compareTo(window) <= 0;
    }
}
