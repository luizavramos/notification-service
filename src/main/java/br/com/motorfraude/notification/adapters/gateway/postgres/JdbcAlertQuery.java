package br.com.motorfraude.notification.adapters.gateway.postgres;

import br.com.motorfraude.contracts.ContextCompleteness;
import br.com.motorfraude.contracts.Severity;
import br.com.motorfraude.notification.application.gateway.AlertQuery;
import br.com.motorfraude.notification.application.usecase.AlertCursor;
import br.com.motorfraude.notification.application.usecase.AlertFilter;
import br.com.motorfraude.notification.application.usecase.AlertSummary;
import java.sql.Array;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.List;
import org.springframework.jdbc.core.simple.JdbcClient;

public class JdbcAlertQuery implements AlertQuery {

    private final JdbcClient jdbc;

    public JdbcAlertQuery(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public List<AlertSummary> search(AlertFilter filter, AlertCursor after, int limit) {
        return jdbc.sql("""
                SELECT alert_id, transaction_id, account_token, severity, score, rule_set_version, context_completeness, detected_at,
                       ARRAY(SELECT r ->> 'ruleId' FROM jsonb_array_elements(payload -> 'triggeredRules') r) AS rule_ids
                FROM alerts
                WHERE (CAST(:account AS text) IS NULL OR account_token = :account)
                  AND (CAST(:severity AS text) IS NULL OR severity = :severity)
                  AND (CAST(:from AS timestamptz) IS NULL OR detected_at >= :from)
                  AND (CAST(:afterAt AS timestamptz) IS NULL OR (detected_at, alert_id) < (:afterAt, :afterId))
                ORDER BY detected_at DESC, alert_id DESC
                LIMIT :limit
                """)
                .param("account", filter.accountId())
                .param("severity", filter.severity() == null ? null : filter.severity().name())
                .param("from", filter.from() == null ? null : JdbcAlertRepository.utc(filter.from()))
                .param("afterAt", after == null ? null : JdbcAlertRepository.utc(after.detectedAt()))
                .param("afterId", after == null ? "" : after.alertId())
                .param("limit", limit)
                .query((rs, n) -> new AlertSummary(rs.getString("alert_id"), rs.getString("transaction_id"), rs.getString("account_token"),
                        Severity.valueOf(rs.getString("severity")), rs.getInt("score"), rs.getLong("rule_set_version"),
                        ContextCompleteness.valueOf(rs.getString("context_completeness")),
                        rs.getObject("detected_at", OffsetDateTime.class).toInstant(), ruleIds(rs.getArray("rule_ids"))))
                .list();
    }

    private static List<String> ruleIds(Array array) throws SQLException {
        return array == null ? List.of() : List.of((String[]) array.getArray());
    }
}
