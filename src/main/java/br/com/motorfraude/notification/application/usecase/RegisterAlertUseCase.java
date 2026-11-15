package br.com.motorfraude.notification.application.usecase;

import br.com.motorfraude.contracts.FraudAlert;
import br.com.motorfraude.contracts.Severity;
import br.com.motorfraude.notification.application.gateway.AlertRepository;
import br.com.motorfraude.notification.application.gateway.DeliveryRepository;
import br.com.motorfraude.notification.application.gateway.DeliveryMetrics;
import br.com.motorfraude.notification.application.gateway.UnitOfWork;
import br.com.motorfraude.notification.domain.ChannelPolicy;
import br.com.motorfraude.notification.domain.Delivery;
import br.com.motorfraude.notification.domain.DeliveryStatus;
import br.com.motorfraude.notification.domain.SuppressionPolicy;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

public final class RegisterAlertUseCase {

    public record Registration(boolean newAlert, List<Delivery> toDispatch) {
    }

    private final AlertRepository alerts;
    private final DeliveryRepository deliveries;
    private final SuppressionPolicy suppression;
    private final UnitOfWork unitOfWork;
    private final Clock clock;
    private final DeliveryMetrics metrics;

    public RegisterAlertUseCase(AlertRepository alerts, DeliveryRepository deliveries, SuppressionPolicy suppression, UnitOfWork unitOfWork,
            Clock clock) {
        this(alerts, deliveries, suppression, unitOfWork, clock, DeliveryMetrics.NONE);
    }

    public RegisterAlertUseCase(AlertRepository alerts, DeliveryRepository deliveries, SuppressionPolicy suppression, UnitOfWork unitOfWork,
            Clock clock, DeliveryMetrics metrics) {
        this.metrics = metrics;
        this.unitOfWork = unitOfWork;
        this.alerts = alerts;
        this.deliveries = deliveries;
        this.suppression = suppression;
        this.clock = clock;
    }

    public Registration register(FraudAlert alert) {
        return unitOfWork.inTransaction(() -> {
            boolean inserted = alerts.insertIfAbsent(alert);
            if (inserted) {
                createDeliveries(alert);
            }
            return new Registration(inserted, deliveries.pendingOf(alert.alertId()));
        });
    }

    private void createDeliveries(FraudAlert alert) {
        var channels = ChannelPolicy.channelsFor(alert.severity());
        Optional<Instant> previous = alert.severity() == Severity.HIGH
                ? deliveries.latestCustomerNotification(alert.accountId(), alert.detectedAt(), suppression.window(), alert.alertId())
                : Optional.empty();
        var now = clock.instant();
        for (var channel : channels) {
            var delivery = suppression.shouldSuppress(channel, previous, alert.detectedAt())
                    ? Delivery.suppressed(alert.alertId(), channel, now)
                    : Delivery.pending(alert.alertId(), channel, now);
            deliveries.insertIfAbsent(delivery);
            if (delivery.status() == DeliveryStatus.SUPPRESSED) {
                metrics.result(channel, "suppressed");
            }
        }
    }
}
