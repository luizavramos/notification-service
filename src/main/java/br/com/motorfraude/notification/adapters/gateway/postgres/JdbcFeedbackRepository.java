package br.com.motorfraude.notification.adapters.gateway.postgres;

import br.com.motorfraude.notification.application.gateway.FeedbackRepository;
import br.com.motorfraude.notification.application.usecase.Verdict;
import org.springframework.jdbc.core.simple.JdbcClient;

public class JdbcFeedbackRepository implements FeedbackRepository {

    private final JdbcClient jdbc;

    public JdbcFeedbackRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public void save(String alertId, Verdict verdict, String analyst) {
        jdbc.sql("INSERT INTO alert_feedback (alert_id, verdict, analyst) VALUES (:alertId, :verdict, :analyst)")
                .param("alertId", alertId).param("verdict", verdict.name()).param("analyst", analyst).update();
    }
}
