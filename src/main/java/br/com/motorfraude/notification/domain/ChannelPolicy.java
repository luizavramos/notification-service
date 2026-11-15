package br.com.motorfraude.notification.domain;

import br.com.motorfraude.contracts.Severity;
import java.util.List;
import java.util.Optional;

public final class ChannelPolicy {

    private ChannelPolicy() {
    }

    public static List<Channel> channelsFor(Severity severity) {
        return switch (severity) {
            case MEDIUM -> List.of(Channel.ANTIFRAUD_WEBHOOK);
            case HIGH -> List.of(Channel.ANTIFRAUD_WEBHOOK, Channel.PUSH, Channel.EMAIL);
            case NONE -> throw new IllegalArgumentException("severidade NONE não é alerta e não tem canal");
        };
    }

    public static Optional<Channel> fallbackFor(Channel failed, Severity severity) {
        return failed == Channel.PUSH && severity == Severity.HIGH ? Optional.of(Channel.SMS) : Optional.empty();
    }
}
