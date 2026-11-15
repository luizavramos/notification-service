package br.com.motorfraude.notification.adapters.gateway.postgres;

import static br.com.motorfraude.notification.TestAlerts.ACCOUNT;
import static br.com.motorfraude.notification.TestAlerts.NOW;
import static br.com.motorfraude.notification.TestAlerts.alert;
import static org.assertj.core.api.Assertions.assertThat;

import br.com.motorfraude.contracts.FraudAlert;
import br.com.motorfraude.contracts.Severity;
import br.com.motorfraude.notification.domain.Channel;
import br.com.motorfraude.notification.domain.Delivery;
import br.com.motorfraude.notification.domain.DeliveryStatus;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class JdbcRepositoriesIT extends PostgresSupport {

    private JdbcAlertRepository alerts;
    private JdbcDeliveryRepository deliveries;

    @BeforeEach
    void repositories() {
        alerts = new JdbcAlertRepository(jdbc);
        deliveries = new JdbcDeliveryRepository(jdbc);
    }

    private static FraudAlert high(String id, Instant at) {
        return alert(id, "tx-" + id + "-abcdef", Severity.HIGH, at);
    }

    @Test
    @DisplayName("a migração cria as tabelas, as chaves e os índices do plano")
    void schema() {
        var tables = jdbc.sql("select tablename from pg_tables where schemaname = 'public'").query(String.class).list();
        var indexes = jdbc.sql("select indexname from pg_indexes where schemaname = 'public'").query(String.class).list();

        assertThat(tables).contains("alerts", "notification_delivery", "alert_feedback");
        assertThat(indexes).anyMatch(name -> name.contains("alerts") && name.contains("account"));
        assertThat(indexes).anyMatch(name -> name.contains("delivery") && name.contains("status"));
        assertThat(jdbc.sql("select relkind from pg_class where relname = 'alerts'").query(String.class).single()).isEqualTo("p");
    }

    @Test
    @DisplayName("H1: grava o alerta; reentregar o mesmo alerta não grava de novo (ON CONFLICT DO NOTHING)")
    void alertaIdempotente() {
        assertThat(alerts.insertIfAbsent(high("a1", NOW))).isTrue();
        assertThat(alerts.insertIfAbsent(high("a1", NOW))).isFalse();

        assertThat(jdbc.sql("select count(*) from alerts").query(Long.class).single()).isEqualTo(1);
    }

    @Test
    @DisplayName("o mesmo transactionId, com outro alertId, no mesmo instante, não grava de novo")
    void mesmaTransacao() {
        assertThat(alerts.insertIfAbsent(alert("a1", "tx-comum-abcdef", Severity.HIGH, NOW))).isTrue();
        assertThat(alerts.insertIfAbsent(alert("a2", "tx-comum-abcdef", Severity.HIGH, NOW))).isFalse();
    }

    @Test
    @DisplayName("o alerta volta idêntico, com as regras e os instantes, e nada de contato do cliente")
    void idaEVolta() {
        var original = high("a1", NOW);
        alerts.insertIfAbsent(original);

        var read = alerts.find("a1").orElseThrow();

        assertThat(read).isEqualTo(original);
        assertThat(alerts.find("nao-existe")).isEmpty();
        assertThat(jdbc.sql("select payload::text from alerts where alert_id = 'a1'").query(String.class).single()).doesNotContain("@");
    }

    @Test
    @DisplayName("alertas de dias diferentes cabem: a partição do dia é criada na hora, e a busca atravessa as partições")
    void particoesPorDia() {
        var hoje = high("a1", NOW);
        var ontem = high("a2", NOW.minus(Duration.ofDays(1)));
        var daqui30 = high("a3", NOW.plus(Duration.ofDays(30)));

        assertThat(alerts.insertIfAbsent(hoje)).isTrue();
        assertThat(alerts.insertIfAbsent(ontem)).isTrue();
        assertThat(alerts.insertIfAbsent(daqui30)).isTrue();

        assertThat(alerts.find("a1")).contains(hoje);
        assertThat(alerts.find("a2")).contains(ontem);
        assertThat(alerts.find("a3")).contains(daqui30);
        var partitions = jdbc.sql("select count(*) from pg_inherits i join pg_class p on p.oid = i.inhparent where p.relname = 'alerts'")
                .query(Long.class).single();
        assertThat(partitions).isEqualTo(3);
    }

    @Test
    @DisplayName("10 instâncias gravando o mesmo alerta ao mesmo tempo: exatamente uma grava")
    void concorrenciaDeAlerta() throws Exception {
        var inserted = new AtomicInteger();
        run(10, () -> {
            if (alerts.insertIfAbsent(high("a1", NOW))) {
                inserted.incrementAndGet();
            }
            return null;
        });

        assertThat(inserted).hasValue(1);
        assertThat(jdbc.sql("select count(*) from alerts").query(Long.class).single()).isEqualTo(1);
    }

    @Test
    @DisplayName("H3: a PK (alert_id, canal) barra a segunda entrega; 20 gravações simultâneas deixam uma só")
    void entregaIdempotente() throws Exception {
        assertThat(deliveries.insertIfAbsent(Delivery.pending("a1", Channel.PUSH, NOW))).isTrue();
        assertThat(deliveries.insertIfAbsent(Delivery.pending("a1", Channel.PUSH, NOW))).isFalse();
        assertThat(deliveries.insertIfAbsent(Delivery.pending("a1", Channel.EMAIL, NOW))).isTrue();

        var inserted = new AtomicInteger();
        run(20, () -> {
            if (deliveries.insertIfAbsent(Delivery.pending("a9", Channel.SMS, NOW))) {
                inserted.incrementAndGet();
            }
            return null;
        });

        assertThat(inserted).hasValue(1);
    }

    @Test
    @DisplayName("a entrega ganha o resultado: update, find e pendingOf")
    void atualizaEConsulta() {
        deliveries.insertIfAbsent(Delivery.pending("a1", Channel.PUSH, NOW));
        deliveries.insertIfAbsent(Delivery.pending("a1", Channel.EMAIL, NOW));
        deliveries.insertIfAbsent(Delivery.suppressed("a1", Channel.SMS, NOW));

        deliveries.update(deliveries.find("a1", Channel.PUSH).orElseThrow().sent("msg-1", NOW.plusSeconds(5)));
        deliveries.update(deliveries.find("a1", Channel.EMAIL).orElseThrow().retryScheduled("503", NOW.plusSeconds(5)));

        var push = deliveries.find("a1", Channel.PUSH).orElseThrow();
        assertThat(push.status()).isEqualTo(DeliveryStatus.SENT);
        assertThat(push.providerMessageId()).isEqualTo("msg-1");
        var email = deliveries.find("a1", Channel.EMAIL).orElseThrow();
        assertThat(email.attempts()).isEqualTo(1);
        assertThat(email.lastError()).isEqualTo("503");
        assertThat(deliveries.pendingOf("a1")).extracting(Delivery::channel).containsExactly(Channel.EMAIL);
        assertThat(deliveries.find("a1", Channel.ANTIFRAUD_WEBHOOK)).isEmpty();
    }

    @Test
    @DisplayName("o reconciliador enxerga só as PENDING paradas há mais que o limite")
    void paradas() {
        deliveries.insertIfAbsent(Delivery.pending("velha", Channel.PUSH, NOW.minus(Duration.ofMinutes(10))));
        deliveries.insertIfAbsent(Delivery.pending("nova", Channel.PUSH, NOW));
        deliveries.insertIfAbsent(new Delivery("enviada", Channel.PUSH, DeliveryStatus.SENT, 0, null, "m", NOW.minus(Duration.ofMinutes(10))));

        var stale = deliveries.stalePending(NOW.minus(Duration.ofMinutes(2)), 100);

        assertThat(stale).extracting(Delivery::alertId).containsExactly("velha");
        assertThat(deliveries.stalePending(NOW.minus(Duration.ofMinutes(2)), 0)).isEmpty();
    }

    private void seed(String alertId, Severity severity, Instant at, String account, DeliveryStatus pushStatus) {
        var base = alert(alertId, "tx-" + alertId + "-abcdef", severity, at);
        var withAccount = new FraudAlert(base.alertId(), base.transactionId(), account, base.severity(), base.score(), base.triggeredRules(),
                base.ruleSetVersion(), base.contextCompleteness(), base.eventReceivedAt(), base.detectedAt(), base.traceId());
        alerts.insertIfAbsent(withAccount);
        deliveries.insertIfAbsent(new Delivery(alertId, Channel.ANTIFRAUD_WEBHOOK, DeliveryStatus.SENT, 0, null, "caso", at));
        deliveries.insertIfAbsent(new Delivery(alertId, Channel.PUSH, pushStatus, 0, null, null, at));
    }

    @Test
    @DisplayName("H5: a consulta da supressão acha o último HIGH notificado (PENDING ou SENT) da conta dentro da janela")
    void supressaoAchaNotificado() {
        seed("a1", Severity.HIGH, NOW.minus(Duration.ofMinutes(5)), ACCOUNT, DeliveryStatus.SENT);
        seed("a2", Severity.HIGH, NOW.minus(Duration.ofMinutes(2)), ACCOUNT, DeliveryStatus.PENDING);

        var found = deliveries.latestCustomerNotification(ACCOUNT, NOW, Duration.ofMinutes(10), "a3");

        assertThat(found).contains(NOW.minus(Duration.ofMinutes(2)));
    }

    @Test
    @DisplayName("a consulta ignora o que não chegou ao cliente, MEDIUM, outra conta, fora da janela e o próprio alerta")
    void supressaoIgnora() {
        seed("falhou", Severity.HIGH, NOW.minus(Duration.ofMinutes(3)), ACCOUNT, DeliveryStatus.FAILED);
        seed("suprimida", Severity.HIGH, NOW.minus(Duration.ofMinutes(3)), ACCOUNT, DeliveryStatus.SUPPRESSED);
        seed("medium", Severity.MEDIUM, NOW.minus(Duration.ofMinutes(3)), ACCOUNT, DeliveryStatus.SENT);
        seed("outra", Severity.HIGH, NOW.minus(Duration.ofMinutes(3)), "acc_tok_00000099", DeliveryStatus.SENT);
        seed("velha", Severity.HIGH, NOW.minus(Duration.ofMinutes(30)), ACCOUNT, DeliveryStatus.SENT);
        seed("propria", Severity.HIGH, NOW, ACCOUNT, DeliveryStatus.SENT);

        assertThat(deliveries.latestCustomerNotification(ACCOUNT, NOW, Duration.ofMinutes(10), "propria")).isEmpty();
    }

    @Test
    @DisplayName("a consulta enxerga também o alerta de um dia anterior (a janela cruza a meia-noite)")
    void supressaoCruzaMeiaNoite() {
        var meiaNoite = Instant.parse("2026-11-16T00:03:00Z");
        seed("a1", Severity.HIGH, Instant.parse("2026-11-15T23:58:00Z"), ACCOUNT, DeliveryStatus.SENT);

        var found = deliveries.latestCustomerNotification(ACCOUNT, meiaNoite, Duration.ofMinutes(10), "a2");

        assertThat(found).contains(Instant.parse("2026-11-15T23:58:00Z"));
    }

    private static void run(int threads, Callable<Void> task) throws Exception {
        try (var executor = Executors.newFixedThreadPool(threads)) {
            var tasks = new ArrayList<Callable<Void>>();
            for (int i = 0; i < threads; i++) {
                tasks.add(task);
            }
            for (var future : executor.invokeAll(tasks)) {
                future.get();
            }
        }
    }
}
