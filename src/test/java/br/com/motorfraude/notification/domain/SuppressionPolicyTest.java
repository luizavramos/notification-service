package br.com.motorfraude.notification.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SuppressionPolicyTest {

    private static final Instant NOW = Instant.parse("2026-11-15T12:00:00Z");
    private final SuppressionPolicy policy = SuppressionPolicy.defaultPolicy();

    @Test
    @DisplayName("a janela padrão é de 10 minutos")
    void janelaPadrao() {
        assertThat(policy.window()).isEqualTo(Duration.ofMinutes(10));
    }

    @Test
    @DisplayName("H5: outro HIGH da mesma conta em até 10 min suprime a entrega ao cliente")
    void suprimeDentroDaJanela() {
        var previous = Optional.of(NOW.minus(Duration.ofMinutes(5)));

        assertThat(policy.shouldSuppress(Channel.PUSH, previous, NOW)).isTrue();
        assertThat(policy.shouldSuppress(Channel.EMAIL, previous, NOW)).isTrue();
        assertThat(policy.shouldSuppress(Channel.SMS, previous, NOW)).isTrue();
    }

    @Test
    @DisplayName("o limite de 10 min exatos ainda suprime; um segundo a mais, não")
    void limite() {
        assertThat(policy.shouldSuppress(Channel.PUSH, Optional.of(NOW.minus(Duration.ofMinutes(10))), NOW)).isTrue();
        assertThat(policy.shouldSuppress(Channel.PUSH, Optional.of(NOW.minus(Duration.ofMinutes(10)).minusSeconds(1)), NOW)).isFalse();
    }

    @Test
    @DisplayName("sem HIGH anterior na janela, não suprime")
    void semAnterior() {
        assertThat(policy.shouldSuppress(Channel.PUSH, Optional.empty(), NOW)).isFalse();
    }

    @Test
    @DisplayName("o antifraude nunca é suprimido: recebe todos os alertas")
    void antifraudeNaoSuprime() {
        assertThat(policy.shouldSuppress(Channel.ANTIFRAUD_WEBHOOK, Optional.of(NOW.minusSeconds(1)), NOW)).isFalse();
    }

    @Test
    @DisplayName("alerta fora de ordem (o anterior é posterior no tempo) também conta pela distância")
    void foraDeOrdem() {
        assertThat(policy.shouldSuppress(Channel.PUSH, Optional.of(NOW.plus(Duration.ofMinutes(3))), NOW)).isTrue();
        assertThat(policy.shouldSuppress(Channel.PUSH, Optional.of(NOW.plus(Duration.ofMinutes(30))), NOW)).isFalse();
    }

    @Test
    @DisplayName("a janela é configurável e precisa ser positiva")
    void janelaConfiguravel() {
        var curta = new SuppressionPolicy(Duration.ofSeconds(30));

        assertThat(curta.shouldSuppress(Channel.PUSH, Optional.of(NOW.minusSeconds(20)), NOW)).isTrue();
        assertThat(curta.shouldSuppress(Channel.PUSH, Optional.of(NOW.minusSeconds(40)), NOW)).isFalse();
        assertThatThrownBy(() -> new SuppressionPolicy(Duration.ZERO)).isInstanceOf(IllegalArgumentException.class);
    }
}
