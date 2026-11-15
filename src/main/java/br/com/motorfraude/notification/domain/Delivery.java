package br.com.motorfraude.notification.domain;

import java.time.Instant;
import java.util.Objects;

public record Delivery(String alertId, Channel channel, DeliveryStatus status, int attempts, String lastError, String providerMessageId,
        Instant updatedAt) {

    public Delivery {
        Objects.requireNonNull(alertId, "alertId");
        Objects.requireNonNull(channel, "channel");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(updatedAt, "updatedAt");
    }

    public static Delivery pending(String alertId, Channel channel, Instant at) {
        return new Delivery(alertId, channel, DeliveryStatus.PENDING, 0, null, null, at);
    }

    public static Delivery suppressed(String alertId, Channel channel, Instant at) {
        return new Delivery(alertId, channel, DeliveryStatus.SUPPRESSED, 0, null, null, at);
    }

    public Delivery sent(String providerMessageId, Instant at) {
        requirePending("marcar como enviada");
        return new Delivery(alertId, channel, DeliveryStatus.SENT, attempts, lastError, providerMessageId, at);
    }

    public Delivery retryScheduled(String error, Instant at) {
        requirePending("agendar nova tentativa");
        return new Delivery(alertId, channel, DeliveryStatus.PENDING, attempts + 1, error, providerMessageId, at);
    }

    public Delivery failed(String error, Instant at) {
        requirePending("marcar como falha");
        return new Delivery(alertId, channel, DeliveryStatus.FAILED, attempts, error, providerMessageId, at);
    }

    public String idempotencyKey() {
        return alertId + ":" + channel.name();
    }

    private void requirePending(String action) {
        if (status != DeliveryStatus.PENDING) {
            throw new IllegalStateException("não é possível " + action + " uma entrega " + status);
        }
    }
}
