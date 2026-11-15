package br.com.motorfraude.notification.fakes;

import br.com.motorfraude.notification.application.gateway.FeedbackRepository;
import br.com.motorfraude.notification.application.usecase.Verdict;
import java.util.ArrayList;
import java.util.List;

public final class InMemoryFeedbackRepository implements FeedbackRepository {

    public record Saved(String alertId, Verdict verdict, String analyst) {
    }

    public final List<Saved> saved = new ArrayList<>();

    @Override
    public void save(String alertId, Verdict verdict, String analyst) {
        saved.add(new Saved(alertId, verdict, analyst));
    }
}
