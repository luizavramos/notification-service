package br.com.motorfraude.notification.infrastructure;

import static br.com.motorfraude.notification.TestAlerts.NOW;
import static br.com.motorfraude.notification.TestAlerts.alert;
import static br.com.motorfraude.notification.TestAlerts.id;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.assertj.core.api.Assertions.assertThat;

import br.com.motorfraude.contracts.FraudAlert;
import br.com.motorfraude.contracts.JsonCodec;
import br.com.motorfraude.contracts.Severity;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ReliabilityIT extends AbstractIntegrationTest {

    private static final String WEBHOOK = "/antifraud-legacy/webhooks/fraud-alerts";
    private static final String PUSH = "/push-provider/v1/push";
    private static final String EMAIL = "/email-provider/v1/email";
    private static final String SMS = "/sms-provider/v1/sms";
    private static final int ALERTS = 1000;

    @Autowired
    JdbcClient jdbc;

    @BeforeEach
    void reset() {
        jdbc.sql("TRUNCATE alerts, notification_delivery, alert_feedback").update();
        PROVIDERS.resetAll();
        PROVIDERS.stubFor(post(urlEqualTo(WEBHOOK)).willReturn(aResponse().withStatus(202).withBody("{\"caseId\":\"CASE-1\"}")));
        PROVIDERS.stubFor(post(urlEqualTo(PUSH)).willReturn(aResponse().withStatus(202).withBody("{\"messageId\":\"push-1\"}")));
        PROVIDERS.stubFor(post(urlEqualTo(EMAIL)).willReturn(aResponse().withStatus(202).withBody("{\"messageId\":\"email-1\"}")));
        PROVIDERS.stubFor(post(urlEqualTo(SMS)).willReturn(aResponse().withStatus(202).withBody("{\"messageId\":\"sms-1\"}")));
        PROVIDERS.stubFor(get(urlPathMatching("/contacts/v1/contacts/.*")).willReturn(aResponse().withStatus(200)
                .withBody("{\"pushToken\":\"push-token\",\"email\":\"cliente@exemplo.invalid\",\"phone\":\"+5511900000000\"}")));
    }

    private static FraudAlert distinct(int n, Severity severity) {
        var base = alert(id(n), "tx-carga-%05d-abcd".formatted(n), severity, NOW.plusMillis(n));
        return new FraudAlert(base.alertId(), base.transactionId(), "acc_tok_%08d".formatted(n), base.severity(), base.score(), base.triggeredRules(),
                base.ruleSetVersion(), base.contextCompleteness(), base.eventReceivedAt(), base.detectedAt(), base.traceId());
    }

    private static void publishAll(List<FraudAlert> alerts) {
        try (var producer = new KafkaProducer<String, String>(Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class))) {
            alerts.forEach(a -> producer.send(new ProducerRecord<>("fraud-alerts.v1", a.accountId(), JsonCodec.toJson(a))));
            producer.flush();
        }
    }

    private String state() {
        return jdbc.sql("SELECT channel || ':' || status || '=' || count(*) FROM notification_delivery GROUP BY channel, status ORDER BY 1").query(String.class).list()
                + " alerts=" + jdbc.sql("SELECT count(*) FROM alerts").query(Long.class).single() + " chamadas=" + PROVIDERS.getAllServeEvents().size();
    }

    private void await(String what, Duration timeout, BooleanSupplier condition) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        throw new AssertionError("não aconteceu em " + timeout + ": " + what + " | " + state());
    }

    private long countStatus(String channel, String status) {
        return jdbc.sql("SELECT count(*) FROM notification_delivery WHERE channel = :c AND status = :s").param("c", channel).param("s", status)
                .query(Long.class).single();
    }

    private static Map<String, Long> callsByKey() {
        return PROVIDERS.findAll(postRequestedFor(urlPathMatching("/(antifraud-legacy|push-provider|email-provider|sms-provider)/.*"))).stream()
                .collect(Collectors.groupingBy(r -> r.getHeader("Idempotency-Key"), Collectors.counting()));
    }

    @Test
    @DisplayName("SC-002: reentregar 1.000 alertas (500 HIGH e 500 MEDIUM) pelo Kafka não gera nenhuma chamada duplicada aos provedores")
    void reentregaDe1000Alertas() {
        var alerts = IntStream.rangeClosed(1, ALERTS).mapToObj(n -> distinct(n, n % 2 == 0 ? Severity.HIGH : Severity.MEDIUM)).toList();

        publishAll(alerts);
        await("todas as entregas SENT", Duration.ofSeconds(150), () -> countStatus("ANTIFRAUD_WEBHOOK", "SENT") == ALERTS
                && countStatus("PUSH", "SENT") == ALERTS / 2 && countStatus("EMAIL", "SENT") == ALERTS / 2);
        int firstPass = PROVIDERS.getAllServeEvents().size();

        publishAll(alerts);
        var marker = distinct(ALERTS + 1, Severity.MEDIUM);
        publishAll(List.of(marker));
        await("marcador processado", Duration.ofSeconds(60), () -> countStatus("ANTIFRAUD_WEBHOOK", "SENT") == ALERTS + 1);
        await("reentrega consumida", Duration.ofSeconds(60), () -> PROVIDERS.getAllServeEvents().size() > firstPass);

        var byKey = callsByKey();
        assertThat(byKey.values()).as("toda Idempotency-Key aparece uma única vez").allMatch(count -> count == 1L);
        assertThat(byKey).hasSize(ALERTS + ALERTS / 2 * 2 + 1);
        assertThat(jdbc.sql("SELECT count(*) FROM alerts").query(Long.class).single()).isEqualTo(ALERTS + 1);
    }

    @Test
    @org.springframework.test.annotation.DirtiesContext(methodMode = org.springframework.test.annotation.DirtiesContext.MethodMode.AFTER_METHOD)
    @DisplayName("SC-003: com o push em 503, 100% dos webhooks do antifraude são entregues e o cliente cai no SMS de reserva")
    void pushForaDoAr() {
        PROVIDERS.stubFor(post(urlEqualTo(PUSH)).willReturn(aResponse().withStatus(503)));
        var alerts = IntStream.rangeClosed(1, 200).mapToObj(n -> distinct(n, Severity.HIGH)).toList();

        long start = System.nanoTime();
        publishAll(alerts);
        await("100% dos webhooks SENT", Duration.ofSeconds(60), () -> countStatus("ANTIFRAUD_WEBHOOK", "SENT") == 200);
        long webhooksDoneMs = Duration.ofNanos(System.nanoTime() - start).toMillis();
        await("push FAILED e SMS SENT para todos", Duration.ofSeconds(90), () -> countStatus("PUSH", "FAILED") == 200 && countStatus("SMS", "SENT") == 200);

        assertThat(countStatus("EMAIL", "SENT")).as("o e-mail não é afetado pelo push").isEqualTo(200);
        assertThat(webhooksDoneMs).as("o antifraude não esperou o push desistir").isLessThan(30_000);
        List<LoggedRequest> webhookCalls = PROVIDERS.findAll(postRequestedFor(urlEqualTo(WEBHOOK)));
        assertThat(webhookCalls).hasSize(200);
        assertThat(webhookCalls.stream().map(r -> r.getHeader("Idempotency-Key")).distinct().count()).isEqualTo(200);
    }
}
