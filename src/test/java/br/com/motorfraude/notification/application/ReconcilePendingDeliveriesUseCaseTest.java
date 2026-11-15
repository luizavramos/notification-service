package br.com.motorfraude.notification.application;

import static br.com.motorfraude.notification.TestAlerts.ACCOUNT;
import static br.com.motorfraude.notification.TestAlerts.NOW;
import static br.com.motorfraude.notification.TestAlerts.high;
import static org.assertj.core.api.Assertions.assertThat;

import br.com.motorfraude.notification.application.gateway.Contact;
import br.com.motorfraude.notification.application.usecase.DeliveryDispatcher;
import br.com.motorfraude.notification.application.usecase.DispatchDeliveryUseCase;
import br.com.motorfraude.notification.application.usecase.ReconcilePendingDeliveriesUseCase;
import br.com.motorfraude.notification.domain.Channel;
import br.com.motorfraude.notification.domain.Delivery;
import br.com.motorfraude.notification.domain.DeliveryStatus;
import br.com.motorfraude.notification.fakes.InMemoryAlertRepository;
import br.com.motorfraude.notification.fakes.InMemoryContactDirectory;
import br.com.motorfraude.notification.fakes.InMemoryDeliveryRepository;
import br.com.motorfraude.notification.fakes.RecordingRetryScheduler;
import br.com.motorfraude.notification.fakes.ScriptedChannel;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ReconcilePendingDeliveriesUseCaseTest {

    private final InMemoryAlertRepository alerts = new InMemoryAlertRepository();
    private final InMemoryDeliveryRepository deliveries = new InMemoryDeliveryRepository(alerts);
    private final InMemoryContactDirectory contacts = new InMemoryContactDirectory();
    private final ScriptedChannel webhook = new ScriptedChannel(Channel.ANTIFRAUD_WEBHOOK);
    private final ScriptedChannel push = new ScriptedChannel(Channel.PUSH);
    private final ScriptedChannel sms = new ScriptedChannel(Channel.SMS);
    private ReconcilePendingDeliveriesUseCase reconciler;

    @BeforeEach
    void setUp() {
        contacts.put(ACCOUNT, new Contact("push-token", "cliente@exemplo.invalid", "+5511900000000"));
        var clock = Clock.fixed(NOW, ZoneOffset.UTC);
        var dispatch = new DispatchDeliveryUseCase(alerts, deliveries,
                Map.of(Channel.ANTIFRAUD_WEBHOOK, webhook, Channel.PUSH, push, Channel.SMS, sms), contacts, new RecordingRetryScheduler(), clock);
        reconciler = new ReconcilePendingDeliveriesUseCase(deliveries, new DeliveryDispatcher(dispatch), clock, DispatchDeliveryUseCase.RETRY_DELAYS,
                Duration.ofMinutes(2), 100);
    }

    private void pending(String alertId, Channel channel, int attempts, Duration age) {
        alerts.insertIfAbsent(high(alertId));
        var delivery = new Delivery(alertId, channel, DeliveryStatus.PENDING, attempts, attempts > 0 ? "503" : null, null, NOW.minus(age));
        deliveries.insertIfAbsent(delivery);
    }

    @Test
    @DisplayName("PENDING nunca despachada (quedas depois de confirmar o offset) e parada há mais de 2 min é despachada")
    void orfa() {
        pending("a1", Channel.ANTIFRAUD_WEBHOOK, 0, Duration.ofMinutes(3));

        var count = reconciler.run();

        assertThat(count).isEqualTo(1);
        assertThat(webhook.calls()).hasSize(1);
        assertThat(deliveries.find("a1", Channel.ANTIFRAUD_WEBHOOK).orElseThrow().status()).isEqualTo(DeliveryStatus.SENT);
    }

    @Test
    @DisplayName("entrega recente não é tocada: o despacho normal ainda pode estar em andamento")
    void recente() {
        pending("a1", Channel.ANTIFRAUD_WEBHOOK, 0, Duration.ofSeconds(90));

        assertThat(reconciler.run()).isZero();
        assertThat(webhook.calls()).isEmpty();
    }

    @Test
    @DisplayName("entrega esperando um retry agendado não é reenviada antes do atraso mais a folga (não fura o backoff)")
    void naoFuraOBackoff() {
        pending("a1", Channel.ANTIFRAUD_WEBHOOK, 1, Duration.ofMinutes(2));
        pending("a2", Channel.PUSH, 3, Duration.ofMinutes(20));

        assertThat(reconciler.run()).isZero();
        assertThat(webhook.calls()).isEmpty();
        assertThat(push.calls()).isEmpty();
    }

    @Test
    @DisplayName("passado o atraso do retry mais a folga e a entrega ainda PENDING, o retry se perdeu: despacha")
    void retryPerdido() {
        pending("a1", Channel.ANTIFRAUD_WEBHOOK, 1, Duration.ofMinutes(4));
        pending("a2", Channel.PUSH, 3, Duration.ofMinutes(33));

        assertThat(reconciler.run()).isEqualTo(2);
        assertThat(webhook.calls()).hasSize(1);
        assertThat(push.calls()).hasSize(1);
    }

    @Test
    @DisplayName("o que já foi enviado ou falhou de vez não volta")
    void estadosFinais() {
        alerts.insertIfAbsent(high("a1"));
        deliveries.insertIfAbsent(new Delivery("a1", Channel.ANTIFRAUD_WEBHOOK, DeliveryStatus.SENT, 0, null, "caso", NOW.minus(Duration.ofHours(1))));
        deliveries.insertIfAbsent(new Delivery("a1", Channel.PUSH, DeliveryStatus.FAILED, 4, "503", null, NOW.minus(Duration.ofHours(1))));

        assertThat(reconciler.run()).isZero();
    }

    @Test
    @DisplayName("respeita o limite por rodada")
    void limite() {
        for (int i = 0; i < 5; i++) {
            pending("a" + i, Channel.ANTIFRAUD_WEBHOOK, 0, Duration.ofMinutes(10));
        }
        var limited = new ReconcilePendingDeliveriesUseCase(deliveries,
                new DeliveryDispatcher(new DispatchDeliveryUseCase(alerts, deliveries, Map.of(Channel.ANTIFRAUD_WEBHOOK, webhook), contacts,
                        new RecordingRetryScheduler(), Clock.fixed(NOW, ZoneOffset.UTC))),
                Clock.fixed(NOW, ZoneOffset.UTC), DispatchDeliveryUseCase.RETRY_DELAYS, Duration.ofMinutes(2), 3);

        assertThat(limited.run()).isEqualTo(3);
        assertThat(Instant.EPOCH).isBefore(NOW);
    }

    @Test
    @DisplayName("uma entrega que falha ao despachar não impede as outras da mesma rodada")
    void isolaFalhas() {
        pending("a1", Channel.ANTIFRAUD_WEBHOOK, 0, Duration.ofMinutes(10));
        pending("a2", Channel.PUSH, 0, Duration.ofMinutes(10));
        contacts.unavailable(true);

        reconciler.run();

        assertThat(webhook.calls()).hasSize(1);
    }
}
