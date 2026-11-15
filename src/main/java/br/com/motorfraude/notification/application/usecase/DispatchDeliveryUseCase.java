package br.com.motorfraude.notification.application.usecase;

import br.com.motorfraude.contracts.FraudAlert;
import br.com.motorfraude.notification.application.gateway.AlertRepository;
import br.com.motorfraude.notification.application.gateway.Contact;
import br.com.motorfraude.notification.application.gateway.ContactDirectory;
import br.com.motorfraude.notification.application.gateway.DeliveryMetrics;
import br.com.motorfraude.notification.application.gateway.DeliveryRepository;
import br.com.motorfraude.notification.application.gateway.NotificationChannel;
import br.com.motorfraude.notification.application.gateway.NotificationChannel.Outbound;
import br.com.motorfraude.notification.application.gateway.RetryScheduler;
import br.com.motorfraude.notification.domain.Channel;
import br.com.motorfraude.notification.domain.ChannelPolicy;
import br.com.motorfraude.notification.domain.Delivery;
import br.com.motorfraude.notification.domain.DeliveryStatus;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class DispatchDeliveryUseCase {

    public enum Kind {
        SKIPPED, SENT, RETRY_SCHEDULED, FAILED, DEAD_LETTERED
    }

    public record DispatchResult(Kind kind, Delivery delivery, List<Delivery> fallbacks) {

        static DispatchResult of(Kind kind, Delivery delivery) {
            return new DispatchResult(kind, delivery, List.of());
        }
    }

    public static final List<Duration> RETRY_DELAYS = List.of(Duration.ofMinutes(1), Duration.ofMinutes(5), Duration.ofMinutes(30));
    public static final String NO_CONTACT = "NO_CONTACT";

    private final AlertRepository alerts;
    private final DeliveryRepository deliveries;
    private final Map<Channel, NotificationChannel> channels;
    private final ContactDirectory contacts;
    private final RetryScheduler scheduler;
    private final Clock clock;
    private final List<Duration> retryDelays;
    private final DeliveryMetrics metrics;

    public DispatchDeliveryUseCase(AlertRepository alerts, DeliveryRepository deliveries, Map<Channel, NotificationChannel> channels,
            ContactDirectory contacts, RetryScheduler scheduler, Clock clock) {
        this(alerts, deliveries, channels, contacts, scheduler, clock, RETRY_DELAYS);
    }

    public DispatchDeliveryUseCase(AlertRepository alerts, DeliveryRepository deliveries, Map<Channel, NotificationChannel> channels,
            ContactDirectory contacts, RetryScheduler scheduler, Clock clock, List<Duration> retryDelays) {
        this(alerts, deliveries, channels, contacts, scheduler, clock, retryDelays, DeliveryMetrics.NONE);
    }

    public DispatchDeliveryUseCase(AlertRepository alerts, DeliveryRepository deliveries, Map<Channel, NotificationChannel> channels,
            ContactDirectory contacts, RetryScheduler scheduler, Clock clock, List<Duration> retryDelays, DeliveryMetrics metrics) {
        this.metrics = metrics;
        this.retryDelays = List.copyOf(retryDelays);
        this.alerts = alerts;
        this.deliveries = deliveries;
        this.channels = channels;
        this.contacts = contacts;
        this.scheduler = scheduler;
        this.clock = clock;
    }

    public DispatchResult dispatch(String alertId, Channel channel) {
        var delivery = deliveries.find(alertId, channel).orElse(null);
        if (delivery == null || delivery.status() != DeliveryStatus.PENDING) {
            return DispatchResult.of(Kind.SKIPPED, delivery);
        }
        var alert = alerts.find(alertId).orElseThrow(() -> new IllegalStateException("entrega sem alerta gravado: " + alertId));
        var adapter = channels.get(channel);
        if (adapter == null) {
            throw new IllegalStateException("nenhum adaptador configurado para o canal " + channel);
        }

        Contact contact = null;
        if (channel.customerFacing()) {
            try {
                contact = contacts.find(alert.accountId()).orElse(null);
            } catch (ContactDirectory.UnavailableException e) {
                return retryable(delivery, alert, "serviço de contatos fora do ar: " + e.getMessage());
            }
            if (contact == null) {
                return finalFailure(delivery, NO_CONTACT);
            }
        }

        var result = adapter.send(new Outbound(alert, contact, delivery.idempotencyKey()));
        return switch (result.status()) {
            case SENT -> {
                var sent = delivery.sent(result.providerMessageId(), clock.instant());
                deliveries.update(sent);
                metrics.result(channel, "sent");
                metrics.alertToDelivery(channel, Duration.between(alert.detectedAt(), clock.instant()));
                yield DispatchResult.of(Kind.SENT, sent);
            }
            case PERMANENT_FAILURE -> finalFailure(delivery, result.error());
            case RETRYABLE_FAILURE -> retryable(delivery, alert, result.error());
        };
    }

    private DispatchResult finalFailure(Delivery delivery, String error) {
        var failed = delivery.failed(error, clock.instant());
        deliveries.update(failed);
        metrics.result(delivery.channel(), "failed");
        return DispatchResult.of(Kind.FAILED, failed);
    }

    private DispatchResult retryable(Delivery delivery, FraudAlert alert, String error) {
        var now = clock.instant();
        if (delivery.attempts() < retryDelays.size()) {
            var delay = retryDelays.get(delivery.attempts());
            var updated = delivery.retryScheduled(error, now);
            deliveries.update(updated);
            scheduler.scheduleRetry(updated, delay);
            metrics.result(delivery.channel(), "retry");
            return DispatchResult.of(Kind.RETRY_SCHEDULED, updated);
        }
        var failed = delivery.failed(error, now);
        deliveries.update(failed);
        scheduler.sendToDeadLetter(failed, error);
        metrics.result(delivery.channel(), "failed");
        metrics.deadLettered(delivery.channel().name());
        var fallbacks = new ArrayList<Delivery>();
        ChannelPolicy.fallbackFor(delivery.channel(), alert.severity()).ifPresent(fallbackChannel -> {
            var fallback = Delivery.pending(alert.alertId(), fallbackChannel, now);
            deliveries.insertIfAbsent(fallback);
            deliveries.find(alert.alertId(), fallbackChannel).filter(d -> d.status() == DeliveryStatus.PENDING).ifPresent(fallbacks::add);
        });
        return new DispatchResult(Kind.DEAD_LETTERED, failed, List.copyOf(fallbacks));
    }
}
