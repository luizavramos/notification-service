package br.com.motorfraude.notification.adapters.gateway.postgres;

import br.com.motorfraude.contracts.FraudAlert;
import br.com.motorfraude.contracts.JsonCodec;
import br.com.motorfraude.notification.application.gateway.AlertRepository;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.jdbc.core.simple.JdbcClient;

public class JdbcAlertRepository implements AlertRepository {

    private final JdbcClient jdbc;
    private final Set<LocalDate> knownPartitions = ConcurrentHashMap.newKeySet();

    public JdbcAlertRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public boolean insertIfAbsent(FraudAlert alert) {
        ensurePartition(LocalDate.ofInstant(alert.detectedAt(), ZoneOffset.UTC));
        int rows = jdbc.sql("""
                INSERT INTO alerts (alert_id, transaction_id, account_token, severity, score, rule_set_version, context_completeness,
                                    event_received_at, detected_at, trace_id, payload)
                VALUES (:alertId, :transactionId, :accountToken, :severity, :score, :ruleSetVersion, :completeness,
                        :receivedAt, :detectedAt, :traceId, CAST(:payload AS jsonb))
                ON CONFLICT DO NOTHING
                """)
                .param("alertId", alert.alertId())
                .param("transactionId", alert.transactionId())
                .param("accountToken", alert.accountId())
                .param("severity", alert.severity().name())
                .param("score", alert.score())
                .param("ruleSetVersion", alert.ruleSetVersion())
                .param("completeness", alert.contextCompleteness().name())
                .param("receivedAt", utc(alert.eventReceivedAt()))
                .param("detectedAt", utc(alert.detectedAt()))
                .param("traceId", alert.traceId())
                .param("payload", JsonCodec.toJson(alert))
                .update();
        return rows > 0;
    }

    @Override
    public Optional<FraudAlert> find(String alertId) {
        return jdbc.sql("SELECT payload::text FROM alerts WHERE alert_id = :alertId LIMIT 1")
                .param("alertId", alertId)
                .query(String.class)
                .optional()
                .map(json -> JsonCodec.fromJson(json, FraudAlert.class));
    }

    private void ensurePartition(LocalDate day) {
        if (knownPartitions.contains(day)) {
            return;
        }
        jdbc.sql("SELECT ensure_alert_partition(:day)").param("day", java.sql.Date.valueOf(day)).query().singleRow();
        knownPartitions.add(day);
    }

    static OffsetDateTime utc(Instant instant) {
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }
}
