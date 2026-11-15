package br.com.motorfraude.notification.adapters.gateway.postgres;

import static br.com.motorfraude.notification.adapters.gateway.postgres.JdbcAlertRepository.utc;

import br.com.motorfraude.notification.application.gateway.DeliveryRepository;
import br.com.motorfraude.notification.domain.Channel;
import br.com.motorfraude.notification.domain.Delivery;
import br.com.motorfraude.notification.domain.DeliveryStatus;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.simple.JdbcClient;

public class JdbcDeliveryRepository implements DeliveryRepository {

    private static final String COLUMNS = "alert_id, channel, status, attempts, last_error, provider_msg_id, updated_at";

    private final JdbcClient jdbc;

    public JdbcDeliveryRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public boolean insertIfAbsent(Delivery delivery) {
        int rows = jdbc.sql("""
                INSERT INTO notification_delivery (alert_id, channel, status, attempts, last_error, provider_msg_id, updated_at)
                VALUES (:alertId, :channel, :status, :attempts, :lastError, :providerMessageId, :updatedAt)
                ON CONFLICT (alert_id, channel) DO NOTHING
                """)
                .param("alertId", delivery.alertId())
                .param("channel", delivery.channel().name())
                .param("status", delivery.status().name())
                .param("attempts", delivery.attempts())
                .param("lastError", delivery.lastError())
                .param("providerMessageId", delivery.providerMessageId())
                .param("updatedAt", utc(delivery.updatedAt()))
                .update();
        return rows > 0;
    }

    @Override
    public Optional<Delivery> find(String alertId, Channel channel) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM notification_delivery WHERE alert_id = :alertId AND channel = :channel")
                .param("alertId", alertId)
                .param("channel", channel.name())
                .query(JdbcDeliveryRepository::map)
                .optional();
    }

    @Override
    public void update(Delivery delivery) {
        jdbc.sql("""
                UPDATE notification_delivery
                   SET status = :status, attempts = :attempts, last_error = :lastError, provider_msg_id = :providerMessageId,
                       updated_at = :updatedAt
                 WHERE alert_id = :alertId AND channel = :channel
                """)
                .param("alertId", delivery.alertId())
                .param("channel", delivery.channel().name())
                .param("status", delivery.status().name())
                .param("attempts", delivery.attempts())
                .param("lastError", delivery.lastError())
                .param("providerMessageId", delivery.providerMessageId())
                .param("updatedAt", utc(delivery.updatedAt()))
                .update();
    }

    @Override
    public List<Delivery> pendingOf(String alertId) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM notification_delivery WHERE alert_id = :alertId AND status = 'PENDING' "
                + "ORDER BY created_at, channel")
                .param("alertId", alertId)
                .query(JdbcDeliveryRepository::map)
                .list();
    }

    @Override
    public List<Delivery> stalePending(Instant olderThan, int limit) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM notification_delivery WHERE status = 'PENDING' AND updated_at < :olderThan "
                + "ORDER BY updated_at LIMIT :limit")
                .param("olderThan", utc(olderThan))
                .param("limit", limit)
                .query(JdbcDeliveryRepository::map)
                .list();
    }

    @Override
    public Optional<Instant> latestCustomerNotification(String accountId, Instant around, Duration window, String excludingAlertId) {
        var customerChannels = Arrays.stream(Channel.values()).filter(Channel::customerFacing).map(Channel::name).toList();
        return jdbc.sql("""
                SELECT max(a.detected_at)
                  FROM alerts a
                 WHERE a.account_token = :accountId
                   AND a.severity = 'HIGH'
                   AND a.alert_id <> :excluding
                   AND a.detected_at BETWEEN :from AND :to
                   AND EXISTS (SELECT 1 FROM notification_delivery d
                                WHERE d.alert_id = a.alert_id
                                  AND d.channel IN (:customerChannels)
                                  AND d.status IN ('PENDING', 'SENT'))
                """)
                .param("accountId", accountId)
                .param("excluding", excludingAlertId)
                .param("from", utc(around.minus(window)))
                .param("to", utc(around.plus(window)))
                .param("customerChannels", customerChannels)
                .query((rs, row) -> Optional.ofNullable(rs.getObject(1, java.time.OffsetDateTime.class)).map(java.time.OffsetDateTime::toInstant))
                .single();
    }

    private static Delivery map(ResultSet rs, int row) throws SQLException {
        return new Delivery(
                rs.getString("alert_id"),
                Channel.valueOf(rs.getString("channel")),
                DeliveryStatus.valueOf(rs.getString("status")),
                rs.getInt("attempts"),
                rs.getString("last_error"),
                rs.getString("provider_msg_id"),
                rs.getObject("updated_at", java.time.OffsetDateTime.class).toInstant());
    }
}
