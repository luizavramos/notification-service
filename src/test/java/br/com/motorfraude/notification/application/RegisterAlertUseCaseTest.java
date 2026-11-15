package br.com.motorfraude.notification.application;

import static br.com.motorfraude.notification.TestAlerts.NOW;
import static br.com.motorfraude.notification.TestAlerts.alert;
import static br.com.motorfraude.notification.TestAlerts.high;
import static br.com.motorfraude.notification.TestAlerts.medium;
import static org.assertj.core.api.Assertions.assertThat;

import br.com.motorfraude.contracts.Severity;
import br.com.motorfraude.notification.application.usecase.RegisterAlertUseCase;
import br.com.motorfraude.notification.domain.Channel;
import br.com.motorfraude.notification.domain.Delivery;
import br.com.motorfraude.notification.domain.DeliveryStatus;
import br.com.motorfraude.notification.domain.SuppressionPolicy;
import br.com.motorfraude.notification.fakes.InMemoryAlertRepository;
import br.com.motorfraude.notification.fakes.InMemoryDeliveryRepository;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RegisterAlertUseCaseTest {

    private final InMemoryAlertRepository alerts = new InMemoryAlertRepository();
    private final InMemoryDeliveryRepository deliveries = new InMemoryDeliveryRepository(alerts);
    private RegisterAlertUseCase useCase;

    @BeforeEach
    void setUp() {
        useCase = new RegisterAlertUseCase(alerts, deliveries, SuppressionPolicy.defaultPolicy(), new br.com.motorfraude.notification.fakes.DirectUnitOfWork(),
                Clock.fixed(NOW, ZoneOffset.UTC));
    }

    private DeliveryStatus statusOf(String alertId, Channel channel) {
        return deliveries.find(alertId, channel).orElseThrow().status();
    }

    @Test
    @DisplayName("H1: alerta MEDIUM grava o alerta e uma entrega PENDING ao antifraude")
    void alertaMedium() {
        var registration = useCase.register(medium("a1"));

        assertThat(registration.newAlert()).isTrue();
        assertThat(alerts.find("a1")).isPresent();
        assertThat(deliveries.all()).extracting(Delivery::channel).containsExactly(Channel.ANTIFRAUD_WEBHOOK);
        assertThat(registration.toDispatch()).extracting(Delivery::channel).containsExactly(Channel.ANTIFRAUD_WEBHOOK);
    }

    @Test
    @DisplayName("H2: alerta HIGH cria as entregas do antifraude, do push e do e-mail")
    void alertaHigh() {
        var registration = useCase.register(high("a1"));

        assertThat(registration.toDispatch()).extracting(Delivery::channel).containsExactly(Channel.ANTIFRAUD_WEBHOOK, Channel.PUSH, Channel.EMAIL);
        assertThat(deliveries.all()).allSatisfy(d -> assertThat(d.status()).isEqualTo(DeliveryStatus.PENDING));
    }

    @Test
    @DisplayName("H3: registrar o mesmo alerta de novo (reentrega do Kafka) não duplica nada e devolve as pendentes para despachar")
    void idempotente() {
        useCase.register(high("a1"));
        var again = useCase.register(high("a1"));

        assertThat(again.newAlert()).isFalse();
        assertThat(alerts.all()).hasSize(1);
        assertThat(deliveries.all()).hasSize(3);
        assertThat(again.toDispatch()).hasSize(3);
    }

    @Test
    @DisplayName("a reentrega só devolve o que ainda está PENDING: o que já foi enviado não volta")
    void reentregaNaoDevolveEnviadas() {
        useCase.register(high("a1"));
        deliveries.update(deliveries.find("a1", Channel.PUSH).orElseThrow().sent("m-1", NOW));

        var again = useCase.register(high("a1"));

        assertThat(again.toDispatch()).extracting(Delivery::channel).containsExactly(Channel.ANTIFRAUD_WEBHOOK, Channel.EMAIL);
    }

    @Test
    @DisplayName("o mesmo transactionId com outro alertId é o mesmo alerta: não grava de novo")
    void mesmaTransacao() {
        useCase.register(alert("a1", "tx-comum-abcdef", Severity.HIGH, NOW));
        var other = useCase.register(alert("a2", "tx-comum-abcdef", Severity.HIGH, NOW));

        assertThat(other.newAlert()).isFalse();
        assertThat(alerts.all()).hasSize(1);
    }

    @Test
    @DisplayName("H5: HIGH da mesma conta 5 min depois de um HIGH notificado: push e e-mail suprimidos, antifraude recebe")
    void supressao() {
        useCase.register(alert("a1", "tx-a1-abcdef", Severity.HIGH, NOW.minus(Duration.ofMinutes(5))));

        useCase.register(alert("a2", "tx-a2-abcdef", Severity.HIGH, NOW));

        assertThat(statusOf("a2", Channel.PUSH)).isEqualTo(DeliveryStatus.SUPPRESSED);
        assertThat(statusOf("a2", Channel.EMAIL)).isEqualTo(DeliveryStatus.SUPPRESSED);
        assertThat(statusOf("a2", Channel.ANTIFRAUD_WEBHOOK)).isEqualTo(DeliveryStatus.PENDING);
    }

    @Test
    @DisplayName("as entregas suprimidas não vão para o despacho")
    void suprimidasNaoSaem() {
        useCase.register(alert("a1", "tx-a1-abcdef", Severity.HIGH, NOW.minus(Duration.ofMinutes(5))));

        var second = useCase.register(alert("a2", "tx-a2-abcdef", Severity.HIGH, NOW));

        assertThat(second.toDispatch()).extracting(Delivery::channel).containsExactly(Channel.ANTIFRAUD_WEBHOOK);
    }

    @Test
    @DisplayName("fora da janela de 10 min não suprime")
    void foraDaJanela() {
        useCase.register(alert("a1", "tx-a1-abcdef", Severity.HIGH, NOW.minus(Duration.ofMinutes(11))));

        useCase.register(alert("a2", "tx-a2-abcdef", Severity.HIGH, NOW));

        assertThat(statusOf("a2", Channel.PUSH)).isEqualTo(DeliveryStatus.PENDING);
    }

    @Test
    @DisplayName("o HIGH anterior que não chegou ao cliente (entregas FAILED) não suprime o próximo")
    void anteriorNaoEntregue() {
        useCase.register(alert("a1", "tx-a1-abcdef", Severity.HIGH, NOW.minus(Duration.ofMinutes(5))));
        deliveries.update(deliveries.find("a1", Channel.PUSH).orElseThrow().failed("NO_CONTACT", NOW));
        deliveries.update(deliveries.find("a1", Channel.EMAIL).orElseThrow().failed("NO_CONTACT", NOW));

        useCase.register(alert("a2", "tx-a2-abcdef", Severity.HIGH, NOW));

        assertThat(statusOf("a2", Channel.PUSH)).isEqualTo(DeliveryStatus.PENDING);
    }

    @Test
    @DisplayName("um MEDIUM anterior não suprime o aviso de um HIGH")
    void mediumNaoSuprime() {
        useCase.register(alert("a1", "tx-a1-abcdef", Severity.MEDIUM, NOW.minus(Duration.ofMinutes(2))));

        useCase.register(alert("a2", "tx-a2-abcdef", Severity.HIGH, NOW));

        assertThat(statusOf("a2", Channel.PUSH)).isEqualTo(DeliveryStatus.PENDING);
    }

    @Test
    @DisplayName("contas diferentes não se suprimem")
    void outraConta() {
        var other = new br.com.motorfraude.contracts.FraudAlert("a1", "tx-a1-abcdef", "acc_tok_00000099", Severity.HIGH, 80,
                high("x").triggeredRules(), 7, high("x").contextCompleteness(), NOW, NOW.minus(Duration.ofMinutes(1)), high("x").traceId());
        useCase.register(other);

        useCase.register(high("a2"));

        assertThat(statusOf("a2", Channel.PUSH)).isEqualTo(DeliveryStatus.PENDING);
    }

    @Test
    @DisplayName("a reentrega não reavalia a supressão: o estado já gravado é mantido")
    void reentregaMantemEstado() {
        useCase.register(alert("a1", "tx-a1-abcdef", Severity.HIGH, NOW.minus(Duration.ofMinutes(5))));
        useCase.register(alert("a2", "tx-a2-abcdef", Severity.HIGH, NOW));

        useCase.register(alert("a2", "tx-a2-abcdef", Severity.HIGH, NOW));

        assertThat(statusOf("a2", Channel.PUSH)).isEqualTo(DeliveryStatus.SUPPRESSED);
    }
}
