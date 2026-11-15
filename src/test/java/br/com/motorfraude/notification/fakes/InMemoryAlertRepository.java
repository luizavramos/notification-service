package br.com.motorfraude.notification.fakes;

import br.com.motorfraude.contracts.FraudAlert;
import br.com.motorfraude.notification.application.gateway.AlertRepository;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public final class InMemoryAlertRepository implements AlertRepository {

    private final Map<String, FraudAlert> alerts = new LinkedHashMap<>();

    @Override
    public boolean insertIfAbsent(FraudAlert alert) {
        boolean transactionTaken = alerts.values().stream().anyMatch(a -> a.transactionId().equals(alert.transactionId()));
        if (alerts.containsKey(alert.alertId()) || transactionTaken) {
            return false;
        }
        alerts.put(alert.alertId(), alert);
        return true;
    }

    @Override
    public Optional<FraudAlert> find(String alertId) {
        return Optional.ofNullable(alerts.get(alertId));
    }

    public Collection<FraudAlert> all() {
        return alerts.values();
    }
}
