package br.com.motorfraude.notification.fakes;

import br.com.motorfraude.contracts.FraudAlert;
import br.com.motorfraude.notification.application.gateway.AlertQuery;
import br.com.motorfraude.notification.application.usecase.AlertCursor;
import br.com.motorfraude.notification.application.usecase.AlertFilter;
import br.com.motorfraude.notification.application.usecase.AlertSummary;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public final class InMemoryAlertQuery implements AlertQuery {

    private final List<FraudAlert> alerts = new ArrayList<>();

    public void add(FraudAlert alert) {
        alerts.add(alert);
    }

    @Override
    public List<AlertSummary> search(AlertFilter filter, AlertCursor after, int limit) {
        return alerts.stream()
                .filter(a -> filter.accountId() == null || a.accountId().equals(filter.accountId()))
                .filter(a -> filter.severity() == null || a.severity() == filter.severity())
                .filter(a -> filter.from() == null || !a.detectedAt().isBefore(filter.from()))
                .sorted(Comparator.comparing(FraudAlert::detectedAt).thenComparing(FraudAlert::alertId).reversed())
                .filter(a -> after == null || a.detectedAt().isBefore(after.detectedAt())
                        || (a.detectedAt().equals(after.detectedAt()) && a.alertId().compareTo(after.alertId()) < 0))
                .limit(limit)
                .map(a -> new AlertSummary(a.alertId(), a.transactionId(), a.accountId(), a.severity(), a.score(), a.ruleSetVersion(),
                        a.contextCompleteness(), a.detectedAt(), a.triggeredRules().stream().map(r -> r.ruleId()).toList()))
                .toList();
    }
}
