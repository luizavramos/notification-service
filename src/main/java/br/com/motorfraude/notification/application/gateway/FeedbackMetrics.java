package br.com.motorfraude.notification.application.gateway;

import br.com.motorfraude.notification.application.usecase.Verdict;

public interface FeedbackMetrics {

    void feedback(Verdict verdict, String ruleId);
}
