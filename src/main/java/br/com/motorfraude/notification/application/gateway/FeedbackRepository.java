package br.com.motorfraude.notification.application.gateway;

import br.com.motorfraude.notification.application.usecase.Verdict;

public interface FeedbackRepository {

    void save(String alertId, Verdict verdict, String analyst);
}
