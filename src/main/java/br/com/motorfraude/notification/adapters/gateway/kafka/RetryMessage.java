package br.com.motorfraude.notification.adapters.gateway.kafka;

import br.com.motorfraude.notification.domain.Channel;
import java.time.Instant;

public record RetryMessage(String alertId, Channel channel, int attempt, Instant dueAt) {
}
