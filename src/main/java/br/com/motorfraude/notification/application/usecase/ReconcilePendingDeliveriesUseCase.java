package br.com.motorfraude.notification.application.usecase;

import br.com.motorfraude.notification.application.gateway.DeliveryRepository;
import br.com.motorfraude.notification.application.gateway.DeliveryMetrics;
import br.com.motorfraude.notification.domain.Delivery;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ReconcilePendingDeliveriesUseCase {

    private static final Logger LOG = LoggerFactory.getLogger(ReconcilePendingDeliveriesUseCase.class);

    private final DeliveryRepository deliveries;
    private final DeliveryDispatcher dispatcher;
    private final Clock clock;
    private final List<Duration> retryDelays;
    private final Duration grace;
    private final int limit;
    private final DeliveryMetrics metrics;

    public ReconcilePendingDeliveriesUseCase(DeliveryRepository deliveries, DeliveryDispatcher dispatcher, Clock clock, List<Duration> retryDelays,
            Duration grace, int limit) {
        this(deliveries, dispatcher, clock, retryDelays, grace, limit, DeliveryMetrics.NONE);
    }

    public ReconcilePendingDeliveriesUseCase(DeliveryRepository deliveries, DeliveryDispatcher dispatcher, Clock clock, List<Duration> retryDelays,
            Duration grace, int limit, DeliveryMetrics metrics) {
        this.metrics = metrics;
        this.deliveries = deliveries;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.retryDelays = List.copyOf(retryDelays);
        this.grace = grace;
        this.limit = limit;
    }

    public int run() {
        var now = clock.instant();
        int dispatched = 0;
        for (var delivery : deliveries.stalePending(now.minus(grace), limit)) {
            if (!due(delivery, now)) {
                continue;
            }
            try {
                dispatcher.dispatch(delivery);
                dispatched++;
            } catch (RuntimeException e) {
                LOG.warn("reconciliador: não foi possível despachar {}:{}; fica para a próxima rodada: {}", delivery.alertId(), delivery.channel(),
                        e.toString());
            }
        }
        if (dispatched > 0) {
            metrics.reconciled(dispatched);
        }
        return dispatched;
    }

    private boolean due(Delivery delivery, java.time.Instant now) {
        var waiting = delivery.attempts() == 0 ? Duration.ZERO : retryDelays.get(Math.min(delivery.attempts(), retryDelays.size()) - 1);
        return !delivery.updatedAt().plus(waiting).plus(grace).isAfter(now);
    }
}
