package br.com.motorfraude.notification.adapters.gateway.micrometer;

import br.com.motorfraude.notification.application.gateway.DeliveryMetrics;
import br.com.motorfraude.notification.domain.Channel;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MicrometerDeliveryMetrics implements DeliveryMetrics {

    public static final String INVALID_ALERT = "INVALID_ALERT";
    private static final List<String> RESULTS = List.of("sent", "failed", "suppressed", "retry");

    private final MeterRegistry registry;
    private final Map<Channel, Timer> latency = new EnumMap<>(Channel.class);
    private final Map<String, Counter> deadLetters = new ConcurrentHashMap<>();
    private final Counter reconciled;

    public MicrometerDeliveryMetrics(MeterRegistry registry) {
        this.registry = registry;
        for (Channel channel : Channel.values()) {
            RESULTS.forEach(result -> deliveries(channel, result));
            deadLetters(channel.name());
            latency.put(channel, Timer.builder("notification.alert_to_delivery")
                    .description("tempo do alerta detectado até a entrega ao canal")
                    .tag("channel", channel.name())
                    .serviceLevelObjectives(Duration.ofMillis(100), Duration.ofMillis(500), Duration.ofSeconds(1), Duration.ofSeconds(2),
                            Duration.ofSeconds(5), Duration.ofSeconds(10), Duration.ofSeconds(30), Duration.ofSeconds(60))
                    .register(registry));
        }
        deadLetters(INVALID_ALERT);
        this.reconciled = Counter.builder("notification.pending.reconciled").register(registry);
    }

    private Counter deliveries(Channel channel, String result) {
        return registry.counter("notification.deliveries", "channel", channel.name(), "result", result);
    }

    private Counter deadLetters(String channel) {
        return deadLetters.computeIfAbsent(channel, c -> registry.counter("notification.dlq", "channel", c));
    }

    @Override
    public void result(Channel channel, String result) {
        deliveries(channel, result).increment();
    }

    @Override
    public void alertToDelivery(Channel channel, Duration elapsed) {
        latency.get(channel).record(elapsed.isNegative() ? Duration.ZERO : elapsed);
    }

    @Override
    public void deadLettered(String channel) {
        deadLetters(channel).increment();
    }

    @Override
    public void reconciled(int count) {
        reconciled.increment(count);
    }
}
