package br.com.motorfraude.notification.application.usecase;

import br.com.motorfraude.contracts.FraudAlert;
import br.com.motorfraude.notification.application.gateway.AlertRepository;
import br.com.motorfraude.notification.application.gateway.FeedbackMetrics;
import br.com.motorfraude.notification.application.gateway.FeedbackRepository;

public class RegisterFeedbackUseCase {

    private static final String UNKNOWN_ANALYST = "desconhecido";

    private final AlertRepository alerts;
    private final FeedbackRepository feedback;
    private final FeedbackMetrics metrics;

    public RegisterFeedbackUseCase(AlertRepository alerts, FeedbackRepository feedback, FeedbackMetrics metrics) {
        this.alerts = alerts;
        this.feedback = feedback;
        this.metrics = metrics;
    }

    public void register(String alertId, Verdict verdict, String analyst) {
        FraudAlert alert = alerts.find(alertId).orElseThrow(() -> new AlertNotFoundException(alertId));
        feedback.save(alertId, verdict, analyst == null || analyst.isBlank() ? UNKNOWN_ANALYST : analyst.strip());
        alert.triggeredRules().forEach(rule -> metrics.feedback(verdict, rule.ruleId()));
    }
}
