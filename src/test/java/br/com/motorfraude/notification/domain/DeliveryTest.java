package br.com.motorfraude.notification.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DeliveryTest {

    private static final Instant T0 = Instant.parse("2026-11-15T12:00:00Z");
    private static final Instant T1 = T0.plusSeconds(30);

    @Test
    @DisplayName("uma entrega nova está PENDING, sem tentativas")
    void nova() {
        var delivery = Delivery.pending("alert-1", Channel.PUSH, T0);

        assertThat(delivery.status()).isEqualTo(DeliveryStatus.PENDING);
        assertThat(delivery.attempts()).isZero();
        assertThat(delivery.alertId()).isEqualTo("alert-1");
        assertThat(delivery.channel()).isEqualTo(Channel.PUSH);
    }

    @Test
    @DisplayName("enviada: SENT, com o identificador do provedor e o instante")
    void enviada() {
        var sent = Delivery.pending("alert-1", Channel.PUSH, T0).sent("msg-9", T1);

        assertThat(sent.status()).isEqualTo(DeliveryStatus.SENT);
        assertThat(sent.providerMessageId()).isEqualTo("msg-9");
        assertThat(sent.updatedAt()).isEqualTo(T1);
    }

    @Test
    @DisplayName("falha com nova tentativa: continua PENDING, conta a tentativa e guarda o erro")
    void falhaComRetry() {
        var retried = Delivery.pending("alert-1", Channel.PUSH, T0).retryScheduled("503 do provedor", T1);

        assertThat(retried.status()).isEqualTo(DeliveryStatus.PENDING);
        assertThat(retried.attempts()).isEqualTo(1);
        assertThat(retried.lastError()).isEqualTo("503 do provedor");
        assertThat(retried.retryScheduled("de novo", T1).attempts()).isEqualTo(2);
    }

    @Test
    @DisplayName("falha definitiva: FAILED com o motivo")
    void falhaDefinitiva() {
        var failed = Delivery.pending("alert-1", Channel.PUSH, T0).failed("NO_CONTACT", T1);

        assertThat(failed.status()).isEqualTo(DeliveryStatus.FAILED);
        assertThat(failed.lastError()).isEqualTo("NO_CONTACT");
    }

    @Test
    @DisplayName("suprimida: nasce SUPPRESSED, sem tentativas")
    void suprimida() {
        var suppressed = Delivery.suppressed("alert-1", Channel.EMAIL, T0);

        assertThat(suppressed.status()).isEqualTo(DeliveryStatus.SUPPRESSED);
        assertThat(suppressed.attempts()).isZero();
    }

    @Test
    @DisplayName("só entrega PENDING muda de estado: SENT, FAILED e SUPPRESSED são finais")
    void estadosFinais() {
        var sent = Delivery.pending("alert-1", Channel.PUSH, T0).sent("msg-9", T1);
        var failed = Delivery.pending("alert-1", Channel.PUSH, T0).failed("x", T1);
        var suppressed = Delivery.suppressed("alert-1", Channel.PUSH, T0);

        assertThatThrownBy(() -> sent.sent("msg-10", T1)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> sent.failed("x", T1)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> failed.retryScheduled("x", T1)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> suppressed.sent("m", T1)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("a chave de idempotência do provedor é alertId:canal")
    void chaveDeIdempotencia() {
        assertThat(Delivery.pending("alert-1", Channel.EMAIL, T0).idempotencyKey()).isEqualTo("alert-1:EMAIL");
    }
}
