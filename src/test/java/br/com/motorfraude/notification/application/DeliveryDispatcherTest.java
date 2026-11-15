package br.com.motorfraude.notification.application;

import static br.com.motorfraude.notification.TestAlerts.ACCOUNT;
import static br.com.motorfraude.notification.TestAlerts.NOW;
import static br.com.motorfraude.notification.TestAlerts.high;
import static org.assertj.core.api.Assertions.assertThat;

import br.com.motorfraude.notification.application.gateway.Contact;
import br.com.motorfraude.notification.application.usecase.DeliveryDispatcher;
import br.com.motorfraude.notification.application.usecase.DispatchDeliveryUseCase;
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
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DeliveryDispatcherTest {

    private final InMemoryAlertRepository alerts = new InMemoryAlertRepository();
    private final InMemoryDeliveryRepository deliveries = new InMemoryDeliveryRepository(alerts);
    private final InMemoryContactDirectory contacts = new InMemoryContactDirectory();
    private final ScriptedChannel push = new ScriptedChannel(Channel.PUSH);
    private final ScriptedChannel sms = new ScriptedChannel(Channel.SMS);
    private final RecordingRetryScheduler scheduler = new RecordingRetryScheduler();

    @Test
    @DisplayName("H4.3: PUSH que esgota num HIGH faz o despachante seguir para a reserva de SMS na mesma chamada")
    void seguePelaReserva() {
        contacts.put(ACCOUNT, new Contact("push-token", "cliente@exemplo.invalid", "+5511900000000"));
        var dispatch = new DispatchDeliveryUseCase(alerts, deliveries, Map.of(Channel.PUSH, push, Channel.SMS, sms), contacts, scheduler,
                Clock.fixed(NOW, ZoneOffset.UTC), List.of(Duration.ofSeconds(1)));
        alerts.insertIfAbsent(high("a1"));
        var pending = Delivery.pending("a1", Channel.PUSH, NOW);
        deliveries.insertIfAbsent(pending);
        push.alwaysFails("503");
        var dispatcher = new DeliveryDispatcher(dispatch);

        dispatcher.dispatch(pending);
        dispatcher.dispatch(deliveries.find("a1", Channel.PUSH).orElseThrow());

        assertThat(deliveries.find("a1", Channel.PUSH).orElseThrow().status()).isEqualTo(DeliveryStatus.FAILED);
        assertThat(deliveries.find("a1", Channel.SMS).orElseThrow().status()).isEqualTo(DeliveryStatus.SENT);
        assertThat(sms.calls()).hasSize(1);
        assertThat(scheduler.deadLetters).hasSize(1);
    }

    @Test
    @DisplayName("entrega que não está mais PENDING não chama ninguém")
    void naoPendente() {
        var dispatch = new DispatchDeliveryUseCase(alerts, deliveries, Map.of(Channel.PUSH, push), contacts, scheduler, Clock.fixed(NOW, ZoneOffset.UTC));
        alerts.insertIfAbsent(high("a1"));
        deliveries.insertIfAbsent(new Delivery("a1", Channel.PUSH, DeliveryStatus.SENT, 0, null, "m", NOW));

        new DeliveryDispatcher(dispatch).dispatch(Delivery.pending("a1", Channel.PUSH, NOW));

        assertThat(push.calls()).isEmpty();
    }
}
