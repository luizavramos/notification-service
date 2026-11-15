package br.com.motorfraude.notification.fakes;

import br.com.motorfraude.contracts.Severity;
import br.com.motorfraude.notification.application.gateway.DeliveryRepository;
import br.com.motorfraude.notification.domain.Channel;
import br.com.motorfraude.notification.domain.Delivery;
import br.com.motorfraude.notification.domain.DeliveryStatus;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class InMemoryDeliveryRepository implements DeliveryRepository {

    private final Map<String, Delivery> deliveries = new LinkedHashMap<>();
    private final InMemoryAlertRepository alerts;

    public InMemoryDeliveryRepository(InMemoryAlertRepository alerts) {
        this.alerts = alerts;
    }

    private static String key(String alertId, Channel channel) {
        return alertId + "|" + channel;
    }

    @Override
    public boolean insertIfAbsent(Delivery delivery) {
        return deliveries.putIfAbsent(key(delivery.alertId(), delivery.channel()), delivery) == null;
    }

    @Override
    public Optional<Delivery> find(String alertId, Channel channel) {
        return Optional.ofNullable(deliveries.get(key(alertId, channel)));
    }

    @Override
    public void update(Delivery delivery) {
        deliveries.put(key(delivery.alertId(), delivery.channel()), delivery);
    }

    @Override
    public List<Delivery> pendingOf(String alertId) {
        return deliveries.values().stream().filter(d -> d.alertId().equals(alertId) && d.status() == DeliveryStatus.PENDING).toList();
    }

    @Override
    public List<Delivery> stalePending(Instant olderThan, int limit) {
        return deliveries.values().stream().filter(d -> d.status() == DeliveryStatus.PENDING && d.updatedAt().isBefore(olderThan)).limit(limit)
                .toList();
    }

    @Override
    public Optional<Instant> latestCustomerNotification(String accountId, Instant around, Duration window, String excludingAlertId) {
        var found = new ArrayList<Instant>();
        for (var delivery : deliveries.values()) {
            if (!delivery.channel().customerFacing() || delivery.alertId().equals(excludingAlertId)) {
                continue;
            }
            if (delivery.status() != DeliveryStatus.PENDING && delivery.status() != DeliveryStatus.SENT) {
                continue;
            }
            alerts.find(delivery.alertId()).filter(a -> a.accountId().equals(accountId) && a.severity() == Severity.HIGH)
                    .map(a -> a.detectedAt()).filter(at -> Duration.between(at, around).abs().compareTo(window) <= 0).ifPresent(found::add);
        }
        return found.stream().max(Comparator.naturalOrder());
    }

    public List<Delivery> all() {
        return List.copyOf(deliveries.values());
    }
}
