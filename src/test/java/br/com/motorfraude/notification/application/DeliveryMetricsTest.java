package br.com.motorfraude.notification.application;

import static br.com.motorfraude.notification.TestAlerts.ACCOUNT;
import static br.com.motorfraude.notification.TestAlerts.NOW;
import static br.com.motorfraude.notification.TestAlerts.alert;
import static br.com.motorfraude.notification.TestAlerts.id;
import static org.assertj.core.api.Assertions.assertThat;

import br.com.motorfraude.contracts.Severity;
import br.com.motorfraude.notification.application.gateway.Contact;
import br.com.motorfraude.notification.application.gateway.NotificationChannel;
import br.com.motorfraude.notification.application.gateway.NotificationChannel.Result;
import br.com.motorfraude.notification.application.usecase.DeliveryDispatcher;
import br.com.motorfraude.notification.application.usecase.DispatchDeliveryUseCase;
import br.com.motorfraude.notification.application.usecase.ReconcilePendingDeliveriesUseCase;
import br.com.motorfraude.notification.application.usecase.RegisterAlertUseCase;
import br.com.motorfraude.notification.domain.Channel;
import br.com.motorfraude.notification.domain.Delivery;
import br.com.motorfraude.notification.domain.DeliveryStatus;
import br.com.motorfraude.notification.domain.SuppressionPolicy;
import br.com.motorfraude.notification.fakes.DirectUnitOfWork;
import br.com.motorfraude.notification.fakes.InMemoryAlertRepository;
import br.com.motorfraude.notification.fakes.InMemoryContactDirectory;
import br.com.motorfraude.notification.fakes.InMemoryDeliveryRepository;
import br.com.motorfraude.notification.fakes.RecordingDeliveryMetrics;
import br.com.motorfraude.notification.fakes.RecordingRetryScheduler;
import br.com.motorfraude.notification.fakes.ScriptedChannel;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DeliveryMetricsTest {

    private final InMemoryAlertRepository alerts = new InMemoryAlertRepository();
    private final InMemoryDeliveryRepository deliveries = new InMemoryDeliveryRepository(alerts);
    private final InMemoryContactDirectory contacts = new InMemoryContactDirectory();
    private final RecordingDeliveryMetrics metrics = new RecordingDeliveryMetrics();
    private final ScriptedChannel webhook = new ScriptedChannel(Channel.ANTIFRAUD_WEBHOOK);
    private final ScriptedChannel push = new ScriptedChannel(Channel.PUSH);
    private final Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
    private DispatchDeliveryUseCase dispatch;

    @BeforeEach
    void setUp() {
        contacts.put(ACCOUNT, new Contact("push-token", "cliente@exemplo.invalid", "+5511900000000"));
        Map<Channel, NotificationChannel> channels = Map.of(Channel.ANTIFRAUD_WEBHOOK, webhook, Channel.PUSH, push);
        dispatch = new DispatchDeliveryUseCase(alerts, deliveries, channels, contacts, new RecordingRetryScheduler(), clock,
                DispatchDeliveryUseCase.RETRY_DELAYS, metrics);
    }

    private void seed(String alertId, Severity severity, Channel channel, int attempts) {
        alerts.insertIfAbsent(alert(alertId, "tx-metrica-" + alertId.substring(alertId.length() - 4), severity, NOW.minusSeconds(3)));
        deliveries.insertIfAbsent(new Delivery(alertId, channel, DeliveryStatus.PENDING, attempts, null, null, NOW));
    }

    @Test
    @DisplayName("entrega enviada conta 'sent' e mede o tempo do alerta detectado até a entrega")
    void enviada() {
        seed(id(1), Severity.MEDIUM, Channel.ANTIFRAUD_WEBHOOK, 0);

        dispatch.dispatch(id(1), Channel.ANTIFRAUD_WEBHOOK);

        assertThat(metrics.results).containsExactly("ANTIFRAUD_WEBHOOK:sent");
        assertThat(metrics.latencies).containsExactly("ANTIFRAUD_WEBHOOK:3000");
        assertThat(metrics.deadLetters).isEmpty();
    }

    @Test
    @DisplayName("falha com nova tentativa conta 'retry' e não mede latência")
    void retry() {
        seed(id(2), Severity.HIGH, Channel.PUSH, 0);
        push.then(Result.retryable("503"));

        dispatch.dispatch(id(2), Channel.PUSH);

        assertThat(metrics.results).containsExactly("PUSH:retry");
        assertThat(metrics.latencies).isEmpty();
    }

    @Test
    @DisplayName("recusa definitiva e contato ausente contam 'failed', sem DLQ")
    void falhaDefinitiva() {
        seed(id(3), Severity.HIGH, Channel.PUSH, 0);
        push.then(Result.permanent("400"));
        dispatch.dispatch(id(3), Channel.PUSH);
        contacts.clear();
        seed(id(4), Severity.HIGH, Channel.PUSH, 0);
        dispatch.dispatch(id(4), Channel.PUSH);

        assertThat(metrics.results).containsExactly("PUSH:failed", "PUSH:failed");
        assertThat(metrics.deadLetters).isEmpty();
    }

    @Test
    @DisplayName("tentativas esgotadas contam 'failed' e uma mensagem na DLQ do canal")
    void dlq() {
        seed(id(5), Severity.HIGH, Channel.PUSH, 3);
        push.then(Result.retryable("503"));

        dispatch.dispatch(id(5), Channel.PUSH);

        assertThat(metrics.results).containsExactly("PUSH:failed");
        assertThat(metrics.deadLetters).containsExactly("PUSH");
    }

    @Test
    @DisplayName("entrega já enviada que chega de novo não conta nada")
    void jaEnviada() {
        alerts.insertIfAbsent(alert(id(6), "tx-metrica-0006", Severity.MEDIUM, NOW));
        deliveries.insertIfAbsent(new Delivery(id(6), Channel.ANTIFRAUD_WEBHOOK, DeliveryStatus.SENT, 0, null, "m", NOW));

        dispatch.dispatch(id(6), Channel.ANTIFRAUD_WEBHOOK);

        assertThat(metrics.results).isEmpty();
        assertThat(metrics.latencies).isEmpty();
    }

    @Test
    @DisplayName("o registro conta 'suppressed' para cada canal ao cliente suprimido")
    void suprimida() {
        var register = new RegisterAlertUseCase(alerts, deliveries, SuppressionPolicy.defaultPolicy(), new DirectUnitOfWork(), clock, metrics);
        register.register(alert(id(7), "tx-metrica-0007", Severity.HIGH, NOW.minus(Duration.ofMinutes(5))));
        metrics.results.clear();

        register.register(alert(id(8), "tx-metrica-0008", Severity.HIGH, NOW));

        assertThat(metrics.results).containsExactlyInAnyOrder("PUSH:suppressed", "EMAIL:suppressed");
    }

    @Test
    @DisplayName("o reconciliador conta quantas entregas PENDING órfãs despachou")
    void reconciliador() {
        seed(id(9), Severity.MEDIUM, Channel.ANTIFRAUD_WEBHOOK, 0);
        deliveries.insertIfAbsent(new Delivery(id(10), Channel.ANTIFRAUD_WEBHOOK, DeliveryStatus.PENDING, 0, null, null, NOW.minus(Duration.ofMinutes(10))));
        alerts.insertIfAbsent(alert(id(10), "tx-metrica-0010", Severity.MEDIUM, NOW));
        var reconciler = new ReconcilePendingDeliveriesUseCase(deliveries, new DeliveryDispatcher(dispatch), clock, List.of(Duration.ofMinutes(1)),
                Duration.ofMinutes(2), 100, metrics);

        int dispatched = reconciler.run();

        assertThat(dispatched).isEqualTo(1);
        assertThat(metrics.reconciled).isEqualTo(1);
    }
}
