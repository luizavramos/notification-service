package br.com.motorfraude.notification.adapters.gateway.micrometer;

import br.com.motorfraude.notification.application.gateway.FeedbackMetrics;
import br.com.motorfraude.notification.application.usecase.Verdict;
import io.micrometer.core.instrument.MeterRegistry;

public class MicrometerFeedbackMetrics implements FeedbackMetrics {

    private final MeterRegistry registry;

    public MicrometerFeedbackMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void feedback(Verdict verdict, String ruleId) {
        registry.counter("alerts.feedback", "verdict", verdict.name(), "rule", ruleId).increment();
    }
}
