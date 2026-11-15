package br.com.motorfraude.notification.fakes;

import br.com.motorfraude.notification.application.gateway.FeedbackMetrics;
import br.com.motorfraude.notification.application.usecase.Verdict;
import java.util.ArrayList;
import java.util.List;

public final class RecordingFeedbackMetrics implements FeedbackMetrics {

    public final List<String> recorded = new ArrayList<>();

    @Override
    public void feedback(Verdict verdict, String ruleId) {
        recorded.add(verdict + ":" + ruleId);
    }
}
