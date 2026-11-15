package br.com.motorfraude.notification.application;

import static br.com.motorfraude.notification.TestAlerts.ACCOUNT;
import static br.com.motorfraude.notification.TestAlerts.NOW;
import static br.com.motorfraude.notification.TestAlerts.high;
import static br.com.motorfraude.notification.TestAlerts.medium;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import br.com.motorfraude.notification.application.gateway.Contact;
import br.com.motorfraude.notification.application.gateway.NotificationChannel.Result;
import br.com.motorfraude.notification.application.usecase.DispatchDeliveryUseCase;
import br.com.motorfraude.notification.application.usecase.DispatchDeliveryUseCase.Kind;
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
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DispatchDeliveryUseCaseTest {

    private final InMemoryAlertRepository alerts = new InMemoryAlertRepository();
    private final InMemoryDeliveryRepository deliveries = new InMemoryDeliveryRepository(alerts);
    private final InMemoryContactDirectory contacts = new InMemoryContactDirectory();
    private final RecordingRetryScheduler scheduler = new RecordingRetryScheduler();
    private final ScriptedChannel webhook = new ScriptedChannel(Channel.ANTIFRAUD_WEBHOOK);
    private final ScriptedChannel push = new ScriptedChannel(Channel.PUSH);
    private final ScriptedChannel email = new ScriptedChannel(Channel.EMAIL);
    private final ScriptedChannel sms = new ScriptedChannel(Channel.SMS);
    private DispatchDeliveryUseCase useCase;

    @BeforeEach
    void setUp() {
        Map<Channel, br.com.motorfraude.notification.application.gateway.NotificationChannel> channels = List.of(webhook, push, email, sms).stream()
                .collect(Collectors.toMap(ScriptedChannel::channel, Function.identity()));
        useCase = new DispatchDeliveryUseCase(alerts, deliveries, channels, contacts, scheduler, Clock.fixed(NOW, ZoneOffset.UTC));
        contacts.put(ACCOUNT, new Contact("push-token", "cliente@exemplo.invalid", "+5511900000000"));
    }

    private void seed(br.com.motorfraude.contracts.FraudAlert alert, Channel... channels) {
        alerts.insertIfAbsent(alert);
        for (var channel : channels) {
            deliveries.insertIfAbsent(Delivery.pending(alert.alertId(), channel, NOW));
        }
    }

    private Delivery delivery(String alertId, Channel channel) {
        return deliveries.find(alertId, channel).orElseThrow();
    }

    @Test
    @DisplayName("H1: webhook do antifraude enviado fica SENT, com a chave alertId:canal, sem buscar contato")
    void webhookEnviado() {
        seed(medium("a1"), Channel.ANTIFRAUD_WEBHOOK);

        var result = useCase.dispatch("a1", Channel.ANTIFRAUD_WEBHOOK);

        assertThat(result.kind()).isEqualTo(Kind.SENT);
        assertThat(delivery("a1", Channel.ANTIFRAUD_WEBHOOK).status()).isEqualTo(DeliveryStatus.SENT);
        assertThat(delivery("a1", Channel.ANTIFRAUD_WEBHOOK).providerMessageId()).isEqualTo("msg-1");
        assertThat(webhook.calls()).hasSize(1);
        assertThat(webhook.calls().getFirst().idempotencyKey()).isEqualTo("a1:ANTIFRAUD_WEBHOOK");
        assertThat(webhook.calls().getFirst().contact()).isNull();
        assertThat(contacts.lookups()).isZero();
    }

    @Test
    @DisplayName("H2: push busca o contato na hora do envio e o entrega ao canal; o contato não é gravado")
    void pushComContato() {
        seed(high("a1"), Channel.PUSH);

        useCase.dispatch("a1", Channel.PUSH);

        assertThat(contacts.lookups()).isEqualTo(1);
        assertThat(push.calls().getFirst().contact().pushToken()).isEqualTo("push-token");
        assertThat(delivery("a1", Channel.PUSH).toString()).doesNotContain("push-token").doesNotContain("exemplo.invalid");
    }

    @Test
    @DisplayName("H3: entrega já SENT não é reenviada")
    void naoReenvia() {
        seed(high("a1"), Channel.PUSH);
        useCase.dispatch("a1", Channel.PUSH);

        var again = useCase.dispatch("a1", Channel.PUSH);

        assertThat(again.kind()).isEqualTo(Kind.SKIPPED);
        assertThat(push.calls()).hasSize(1);
    }

    @Test
    @DisplayName("entrega inexistente ou suprimida é ignorada")
    void ignora() {
        seed(high("a1"));
        deliveries.insertIfAbsent(Delivery.suppressed("a1", Channel.PUSH, NOW));

        assertThat(useCase.dispatch("a1", Channel.PUSH).kind()).isEqualTo(Kind.SKIPPED);
        assertThat(useCase.dispatch("a1", Channel.EMAIL).kind()).isEqualTo(Kind.SKIPPED);
        assertThat(push.calls()).isEmpty();
    }

    @Test
    @DisplayName("falha do canal agenda o retry em 1 min, depois 5 min, depois 30 min, mantendo a entrega PENDING")
    void retryComBackoff() {
        seed(medium("a1"), Channel.ANTIFRAUD_WEBHOOK);
        webhook.alwaysFails("503");

        var first = useCase.dispatch("a1", Channel.ANTIFRAUD_WEBHOOK);
        useCase.dispatch("a1", Channel.ANTIFRAUD_WEBHOOK);
        useCase.dispatch("a1", Channel.ANTIFRAUD_WEBHOOK);

        assertThat(first.kind()).isEqualTo(Kind.RETRY_SCHEDULED);
        assertThat(scheduler.retries).extracting(RecordingRetryScheduler.Scheduled::delay)
                .containsExactly(Duration.ofMinutes(1), Duration.ofMinutes(5), Duration.ofMinutes(30));
        assertThat(delivery("a1", Channel.ANTIFRAUD_WEBHOOK).status()).isEqualTo(DeliveryStatus.PENDING);
        assertThat(delivery("a1", Channel.ANTIFRAUD_WEBHOOK).attempts()).isEqualTo(3);
    }

    @Test
    @DisplayName("H4: esgotadas as tentativas, a entrega vira FAILED e vai para a DLQ")
    void esgotou() {
        seed(medium("a1"), Channel.ANTIFRAUD_WEBHOOK);
        webhook.alwaysFails("503");
        for (int i = 0; i < 3; i++) {
            useCase.dispatch("a1", Channel.ANTIFRAUD_WEBHOOK);
        }

        var last = useCase.dispatch("a1", Channel.ANTIFRAUD_WEBHOOK);

        assertThat(last.kind()).isEqualTo(Kind.DEAD_LETTERED);
        assertThat(delivery("a1", Channel.ANTIFRAUD_WEBHOOK).status()).isEqualTo(DeliveryStatus.FAILED);
        assertThat(scheduler.retries).hasSize(3);
        assertThat(scheduler.deadLetters).hasSize(1);
        assertThat(scheduler.deadLetters.getFirst().reason()).contains("503");
        assertThat(last.fallbacks()).isEmpty();
    }

    @Test
    @DisplayName("H4: PUSH esgotado num alerta HIGH cria a entrega SMS como reserva e a devolve para despachar")
    void reservaDeSms() {
        seed(high("a1"), Channel.PUSH);
        push.alwaysFails("503");
        for (int i = 0; i < 3; i++) {
            useCase.dispatch("a1", Channel.PUSH);
        }

        var last = useCase.dispatch("a1", Channel.PUSH);

        assertThat(last.kind()).isEqualTo(Kind.DEAD_LETTERED);
        assertThat(last.fallbacks()).extracting(Delivery::channel).containsExactly(Channel.SMS);
        assertThat(delivery("a1", Channel.SMS).status()).isEqualTo(DeliveryStatus.PENDING);
    }

    @Test
    @DisplayName("e-mail esgotado não tem reserva")
    void emailSemReserva() {
        seed(high("a1"), Channel.EMAIL);
        email.alwaysFails("503");
        for (int i = 0; i < 4; i++) {
            useCase.dispatch("a1", Channel.EMAIL);
        }

        assertThat(deliveries.find("a1", Channel.SMS)).isEmpty();
    }

    @Test
    @DisplayName("a reserva de SMS é despachada como qualquer entrega e fica SENT")
    void smsReservaEnviado() {
        seed(high("a1"), Channel.PUSH);
        push.alwaysFails("503");
        for (int i = 0; i < 4; i++) {
            useCase.dispatch("a1", Channel.PUSH);
        }

        var result = useCase.dispatch("a1", Channel.SMS);

        assertThat(result.kind()).isEqualTo(Kind.SENT);
        assertThat(delivery("a1", Channel.SMS).status()).isEqualTo(DeliveryStatus.SENT);
        assertThat(sms.calls().getFirst().idempotencyKey()).isEqualTo("a1:SMS");
    }

    @Test
    @DisplayName("contato ausente: FAILED com NO_CONTACT, sem chamar o canal e sem retry")
    void semContato() {
        seed(high("a1"), Channel.PUSH);
        contacts.put(ACCOUNT, null);

        var result = useCase.dispatch("a1", Channel.PUSH);

        assertThat(result.kind()).isEqualTo(Kind.FAILED);
        assertThat(delivery("a1", Channel.PUSH).status()).isEqualTo(DeliveryStatus.FAILED);
        assertThat(delivery("a1", Channel.PUSH).lastError()).isEqualTo("NO_CONTACT");
        assertThat(push.calls()).isEmpty();
        assertThat(scheduler.retries).isEmpty();
        assertThat(scheduler.deadLetters).isEmpty();
    }

    @Test
    @DisplayName("serviço de contatos fora do ar é falha com nova tentativa, não NO_CONTACT")
    void contatosForaDoAr() {
        seed(high("a1"), Channel.PUSH);
        contacts.unavailable(true);

        var result = useCase.dispatch("a1", Channel.PUSH);

        assertThat(result.kind()).isEqualTo(Kind.RETRY_SCHEDULED);
        assertThat(delivery("a1", Channel.PUSH).lastError()).contains("contatos");
        assertThat(push.calls()).isEmpty();
    }

    @Test
    @DisplayName("recusa definitiva do provedor: FAILED na hora, sem retry")
    void recusaDefinitiva() {
        seed(high("a1"), Channel.EMAIL);
        email.then(Result.permanent("400 e-mail inválido"));

        var result = useCase.dispatch("a1", Channel.EMAIL);

        assertThat(result.kind()).isEqualTo(Kind.FAILED);
        assertThat(delivery("a1", Channel.EMAIL).lastError()).contains("400");
        assertThat(scheduler.retries).isEmpty();
    }

    @Test
    @DisplayName("sucesso depois de uma falha: a entrega termina SENT e mantém a contagem de tentativas")
    void sucessoDepoisDeFalha() {
        seed(medium("a1"), Channel.ANTIFRAUD_WEBHOOK);
        webhook.then(Result.retryable("503")).then(Result.sent("caso-77"));

        useCase.dispatch("a1", Channel.ANTIFRAUD_WEBHOOK);
        var second = useCase.dispatch("a1", Channel.ANTIFRAUD_WEBHOOK);

        assertThat(second.kind()).isEqualTo(Kind.SENT);
        assertThat(delivery("a1", Channel.ANTIFRAUD_WEBHOOK).providerMessageId()).isEqualTo("caso-77");
        assertThat(delivery("a1", Channel.ANTIFRAUD_WEBHOOK).attempts()).isEqualTo(1);
    }

    @Test
    @DisplayName("canal sem adaptador configurado é erro de configuração, não falha silenciosa")
    void semAdaptador() {
        seed(medium("a1"), Channel.ANTIFRAUD_WEBHOOK);
        var incomplete = new DispatchDeliveryUseCase(alerts, deliveries, Map.of(), contacts, scheduler, Clock.fixed(NOW, ZoneOffset.UTC));

        assertThatThrownBy(() -> incomplete.dispatch("a1", Channel.ANTIFRAUD_WEBHOOK)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ANTIFRAUD_WEBHOOK");
    }
}
