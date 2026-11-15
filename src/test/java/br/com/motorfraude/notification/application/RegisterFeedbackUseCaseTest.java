package br.com.motorfraude.notification.application;

import static br.com.motorfraude.notification.TestAlerts.id;
import static br.com.motorfraude.notification.TestAlerts.high;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import br.com.motorfraude.notification.application.usecase.AlertNotFoundException;
import br.com.motorfraude.notification.application.usecase.RegisterFeedbackUseCase;
import br.com.motorfraude.notification.application.usecase.Verdict;
import br.com.motorfraude.notification.fakes.InMemoryAlertRepository;
import br.com.motorfraude.notification.fakes.InMemoryFeedbackRepository;
import br.com.motorfraude.notification.fakes.RecordingFeedbackMetrics;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RegisterFeedbackUseCaseTest {

    private final InMemoryAlertRepository alerts = new InMemoryAlertRepository();
    private final InMemoryFeedbackRepository feedback = new InMemoryFeedbackRepository();
    private final RecordingFeedbackMetrics metrics = new RecordingFeedbackMetrics();
    private final RegisterFeedbackUseCase useCase = new RegisterFeedbackUseCase(alerts, feedback, metrics);

    @Test
    @DisplayName("grava o veredito do analista e conta um por regra que disparou o alerta")
    void grava() {
        alerts.insertIfAbsent(high(id(1)));

        useCase.register(id(1), Verdict.FALSE_POSITIVE, "ana");

        assertThat(feedback.saved).containsExactly(new InMemoryFeedbackRepository.Saved(id(1), Verdict.FALSE_POSITIVE, "ana"));
        assertThat(metrics.recorded).containsExactly("FALSE_POSITIVE:R10");
    }

    @Test
    @DisplayName("alerta inexistente: nada é gravado nem contado")
    void inexistente() {
        assertThatThrownBy(() -> useCase.register(id(9), Verdict.CONFIRMED_FRAUD, "ana")).isInstanceOf(AlertNotFoundException.class);

        assertThat(feedback.saved).isEmpty();
        assertThat(metrics.recorded).isEmpty();
    }

    @Test
    @DisplayName("o analista pode mudar de ideia: o segundo veredito é gravado ao lado do primeiro")
    void mudaDeIdeia() {
        alerts.insertIfAbsent(high(id(2)));

        useCase.register(id(2), Verdict.FALSE_POSITIVE, "ana");
        useCase.register(id(2), Verdict.CONFIRMED_FRAUD, "ana");

        assertThat(feedback.saved).hasSize(2);
    }

    @Test
    @DisplayName("analista em branco vira 'desconhecido', porque a coluna é obrigatória")
    void semAnalista() {
        alerts.insertIfAbsent(high(id(3)));

        useCase.register(id(3), Verdict.CONFIRMED_FRAUD, " ");

        assertThat(feedback.saved.getFirst().analyst()).isEqualTo("desconhecido");
    }
}
