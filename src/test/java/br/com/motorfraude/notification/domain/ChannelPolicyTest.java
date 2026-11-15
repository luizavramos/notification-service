package br.com.motorfraude.notification.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import br.com.motorfraude.contracts.Severity;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ChannelPolicyTest {

    @Test
    @DisplayName("MEDIUM vai só para o antifraude")
    void medium() {
        assertThat(ChannelPolicy.channelsFor(Severity.MEDIUM)).containsExactly(Channel.ANTIFRAUD_WEBHOOK);
    }

    @Test
    @DisplayName("HIGH vai para o antifraude, o push e o e-mail, nessa ordem")
    void high() {
        assertThat(ChannelPolicy.channelsFor(Severity.HIGH)).containsExactly(Channel.ANTIFRAUD_WEBHOOK, Channel.PUSH, Channel.EMAIL);
    }

    @Test
    @DisplayName("NONE não é alerta: não tem canal")
    void none() {
        assertThatThrownBy(() -> ChannelPolicy.channelsFor(Severity.NONE)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("o SMS é a reserva do PUSH, só em alerta HIGH")
    void reservaDoPush() {
        assertThat(ChannelPolicy.fallbackFor(Channel.PUSH, Severity.HIGH)).contains(Channel.SMS);
        assertThat(ChannelPolicy.fallbackFor(Channel.PUSH, Severity.MEDIUM)).isEmpty();
        assertThat(ChannelPolicy.fallbackFor(Channel.EMAIL, Severity.HIGH)).isEmpty();
        assertThat(ChannelPolicy.fallbackFor(Channel.ANTIFRAUD_WEBHOOK, Severity.HIGH)).isEmpty();
        assertThat(ChannelPolicy.fallbackFor(Channel.SMS, Severity.HIGH)).isEmpty();
    }

    @Test
    @DisplayName("push, e-mail e SMS são canais do cliente; o webhook do antifraude não")
    void canaisDoCliente() {
        assertThat(Channel.PUSH.customerFacing()).isTrue();
        assertThat(Channel.EMAIL.customerFacing()).isTrue();
        assertThat(Channel.SMS.customerFacing()).isTrue();
        assertThat(Channel.ANTIFRAUD_WEBHOOK.customerFacing()).isFalse();
    }
}
