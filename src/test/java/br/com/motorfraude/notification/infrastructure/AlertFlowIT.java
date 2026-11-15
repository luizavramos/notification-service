package br.com.motorfraude.notification.infrastructure;

import static br.com.motorfraude.notification.TestAlerts.ACCOUNT;
import static br.com.motorfraude.notification.TestAlerts.NOW;
import static br.com.motorfraude.notification.TestAlerts.alert;
import static br.com.motorfraude.notification.TestAlerts.id;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;

import br.com.motorfraude.contracts.FraudAlert;
import br.com.motorfraude.contracts.JsonCodec;
import br.com.motorfraude.contracts.Severity;
import br.com.motorfraude.notification.application.usecase.ReconcilePendingDeliveriesUseCase;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class AlertFlowIT extends AbstractIntegrationTest {

    private static final String WEBHOOK = "/antifraud-legacy/webhooks/fraud-alerts";
    private static final String PUSH = "/push-provider/v1/push";
    private static final String EMAIL = "/email-provider/v1/email";
    private static final String SMS = "/sms-provider/v1/sms";
    private static final String CONTACT = "/contacts/v1/contacts/" + ACCOUNT;

    @Autowired
    JdbcClient jdbc;

    @Autowired
    ReconcilePendingDeliveriesUseCase reconciler;

    @BeforeEach
    void reset() {
        jdbc.sql("TRUNCATE alerts, notification_delivery, alert_feedback").update();
        PROVIDERS.resetAll();
        PROVIDERS.stubFor(post(urlEqualTo(WEBHOOK)).willReturn(aResponse().withStatus(202).withBody("{\"caseId\":\"CASE-1\"}")));
        PROVIDERS.stubFor(post(urlEqualTo(PUSH)).willReturn(aResponse().withStatus(202).withBody("{\"messageId\":\"push-1\"}")));
        PROVIDERS.stubFor(post(urlEqualTo(EMAIL)).willReturn(aResponse().withStatus(202).withBody("{\"messageId\":\"email-1\"}")));
        PROVIDERS.stubFor(post(urlEqualTo(SMS)).willReturn(aResponse().withStatus(202).withBody("{\"messageId\":\"sms-1\"}")));
        PROVIDERS.stubFor(get(urlEqualTo(CONTACT)).willReturn(aResponse().withStatus(200)
                .withBody("{\"pushToken\":\"push-token\",\"email\":\"cliente@exemplo.invalid\",\"phone\":\"+5511900000000\"}")));
    }

    private static void publish(String key, String value) {
        try (var producer = new KafkaProducer<String, String>(Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class))) {
            producer.send(new ProducerRecord<>("fraud-alerts.v1", key, value)).get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static void publish(FraudAlert alert) {
        publish(alert.accountId(), JsonCodec.toJson(alert));
    }

    private static void await(String what, BooleanSupplier condition) {
        long deadline = System.nanoTime() + Duration.ofSeconds(40).toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        throw new AssertionError("não aconteceu em 40 s: " + what);
    }

    private String status(String alertId, String channel) {
        return jdbc.sql("SELECT status FROM notification_delivery WHERE alert_id = :a AND channel = :c").param("a", alertId).param("c", channel)
                .query(String.class).optional().orElse("-");
    }

    private int calls(String path) {
        return PROVIDERS.findAll(postRequestedFor(urlEqualTo(path))).size();
    }

    @Test
    @DisplayName("H1: alerta MEDIUM no tópico vira uma linha em alerts e uma chamada assinada ao webhook do antifraude, e a entrega fica SENT")
    void medium() {
        var alert = alert(id(1), "tx-medium-0001", Severity.MEDIUM, NOW);

        publish(alert);

        await("webhook SENT", () -> "SENT".equals(status(id(1), "ANTIFRAUD_WEBHOOK")));
        assertThat(jdbc.sql("SELECT count(*) FROM alerts WHERE alert_id = :a").param("a", id(1)).query(Long.class).single()).isEqualTo(1);
        var request = PROVIDERS.findAll(postRequestedFor(urlEqualTo(WEBHOOK))).getFirst();
        assertThat(request.getHeader("Idempotency-Key")).isEqualTo(id(1) + ":ANTIFRAUD_WEBHOOK");
        assertThat(request.getHeader("X-Signature")).matches("[0-9a-f]{64}");
        assertThat(calls(PUSH)).isZero();
        assertThat(calls(EMAIL)).isZero();
    }

    @Test
    @DisplayName("H2: alerta HIGH chega ao antifraude, ao push e ao e-mail; o contato é buscado no envio e nada de contato fica no banco")
    void high() {
        publish(alert(id(2), "tx-high-0002", Severity.HIGH, NOW));

        await("as 3 entregas SENT", () -> "SENT".equals(status(id(2), "ANTIFRAUD_WEBHOOK")) && "SENT".equals(status(id(2), "PUSH"))
                && "SENT".equals(status(id(2), "EMAIL")));
        assertThat(PROVIDERS.findAll(postRequestedFor(urlEqualTo(PUSH))).getFirst().getBodyAsString()).contains("push-token");
        assertThat(PROVIDERS.findAll(getRequestedFor(urlEqualTo(CONTACT)))).hasSize(2);
        var stored = jdbc.sql("SELECT payload::text || coalesce(string_agg(d.last_error || d.provider_msg_id, ''), '') FROM alerts a "
                + "LEFT JOIN notification_delivery d ON d.alert_id = a.alert_id GROUP BY a.payload::text").query(String.class).single();
        assertThat(stored).doesNotContain("cliente@exemplo.invalid").doesNotContain("push-token").doesNotContain("+5511900000000");
    }

    @Test
    @DisplayName("H3: o mesmo alerta entregue duas vezes pelo Kafka não gera nenhuma chamada nova ao provedor")
    void reentrega() {
        var alert = alert(id(3), "tx-reentrega-0003", Severity.HIGH, NOW);
        publish(alert);
        await("3 entregas SENT", () -> "SENT".equals(status(id(3), "PUSH")) && "SENT".equals(status(id(3), "EMAIL"))
                && "SENT".equals(status(id(3), "ANTIFRAUD_WEBHOOK")));

        publish(alert);
        var marker = alert(id(4), "tx-marcador-0004", Severity.MEDIUM, NOW.plusSeconds(1));
        publish(marker);
        await("marcador processado", () -> "SENT".equals(status(id(4), "ANTIFRAUD_WEBHOOK")));

        assertThat(calls(PUSH)).isEqualTo(1);
        assertThat(calls(EMAIL)).isEqualTo(1);
        assertThat(PROVIDERS.findAll(postRequestedFor(urlEqualTo(WEBHOOK))).stream()
                .filter(r -> (id(3) + ":ANTIFRAUD_WEBHOOK").equals(r.getHeader("Idempotency-Key"))).count()).isEqualTo(1);
    }

    @Test
    @DisplayName("H4: com o push em 503, o antifraude e o e-mail seguem; o push passa por 3 retries, vai para a DLQ e o SMS entra como reserva")
    void pushForaDoAr() {
        PROVIDERS.stubFor(post(urlEqualTo(PUSH)).willReturn(aResponse().withStatus(503)));

        publish(alert(id(5), "tx-push-fora-0005", Severity.HIGH, NOW));

        await("webhook e e-mail SENT (isolamento)", () -> "SENT".equals(status(id(5), "ANTIFRAUD_WEBHOOK")) && "SENT".equals(status(id(5), "EMAIL")));
        await("push FAILED e SMS SENT", () -> "FAILED".equals(status(id(5), "PUSH")) && "SENT".equals(status(id(5), "SMS")));

        assertThat(calls(PUSH)).as("1 tentativa + 3 retries pelos tópicos").isEqualTo(4);
        assertThat(calls(SMS)).isEqualTo(1);
        assertThat(jdbc.sql("SELECT attempts FROM notification_delivery WHERE alert_id = :a AND channel = 'PUSH'").param("a", id(5)).query(Integer.class)
                .single()).isEqualTo(3);
        var dlq = readTopic("alerts.dlq");
        assertThat(dlq).anyMatch(v -> v.contains(id(5)) && v.contains("PUSH"));
    }

    @Test
    @DisplayName("H5: dois HIGH da mesma conta em 5 min: o cliente é avisado só do primeiro, e o antifraude recebe os dois")
    void supressao() {
        publish(alert(id(6), "tx-supr-0006", Severity.HIGH, NOW));
        await("1º HIGH entregue", () -> "SENT".equals(status(id(6), "PUSH")) && "SENT".equals(status(id(6), "EMAIL")));

        publish(alert(id(7), "tx-supr-0007", Severity.HIGH, NOW.plus(Duration.ofMinutes(5))));

        await("2º HIGH tratado", () -> "SENT".equals(status(id(7), "ANTIFRAUD_WEBHOOK")));
        assertThat(status(id(7), "PUSH")).isEqualTo("SUPPRESSED");
        assertThat(status(id(7), "EMAIL")).isEqualTo("SUPPRESSED");
        assertThat(calls(PUSH)).isEqualTo(1);
        assertThat(calls(WEBHOOK)).isEqualTo(2);
    }

    @Test
    @DisplayName("borda: cliente sem contato: PUSH e e-mail ficam FAILED com NO_CONTACT, sem retry; o antifraude recebe")
    void semContato() {
        PROVIDERS.stubFor(get(urlEqualTo(CONTACT)).willReturn(aResponse().withStatus(404)));

        publish(alert(id(8), "tx-sem-contato-0008", Severity.HIGH, NOW));

        await("entregas ao cliente FAILED", () -> "FAILED".equals(status(id(8), "PUSH")) && "FAILED".equals(status(id(8), "EMAIL")));
        assertThat(jdbc.sql("SELECT last_error FROM notification_delivery WHERE alert_id = :a AND channel = 'PUSH'").param("a", id(8))
                .query(String.class).single()).isEqualTo("NO_CONTACT");
        assertThat(calls(PUSH)).isZero();
        assertThat(status(id(8), "ANTIFRAUD_WEBHOOK")).isIn("SENT", "PENDING");
    }

    @Test
    @DisplayName("borda: mensagem que não é um alerta válido vai para a DLQ com o motivo e a partição segue")
    void mensagemInvalida() {
        publish(ACCOUNT, "{isto não é um alerta");
        publish(alert(id(9), "tx-depois-0009", Severity.MEDIUM, NOW));

        await("alerta seguinte processado", () -> "SENT".equals(status(id(9), "ANTIFRAUD_WEBHOOK")));
        assertThat(readTopic("alerts.dlq")).anyMatch(v -> v.contains("isto não é um alerta"));
    }

    @Test
    @DisplayName("T520: o reconciliador despacha a entrega PENDING órfã (queda depois de confirmar o offset) e não toca a que espera retry")
    void reconciliador() {
        var orphan = alert(id(10), "tx-orfa-0010", Severity.MEDIUM, NOW);
        jdbc.sql("""
                INSERT INTO alerts (alert_id, transaction_id, account_token, severity, score, rule_set_version, context_completeness,
                                    event_received_at, detected_at, trace_id, payload)
                VALUES (:id, :tx, :acc, 'MEDIUM', 50, 7, 'FULL', now(), now(), :trace, CAST(:payload AS jsonb))
                """).param("id", id(10)).param("tx", "tx-orfa-0010").param("acc", ACCOUNT).param("trace", orphan.traceId())
                .param("payload", JsonCodec.toJson(orphan)).update();
        var old = OffsetDateTime.ofInstant(Instant.now().minus(Duration.ofMinutes(10)), ZoneOffset.UTC);
        jdbc.sql("INSERT INTO notification_delivery (alert_id, channel, status, attempts, updated_at) VALUES (:id, 'ANTIFRAUD_WEBHOOK', 'PENDING', 0, :at)")
                .param("id", id(10)).param("at", old).update();
        PROVIDERS.resetRequests();

        int dispatched = reconciler.run();

        assertThat(dispatched).isEqualTo(1);
        await("webhook SENT", () -> "SENT".equals(status(id(10), "ANTIFRAUD_WEBHOOK")));
        assertThat(calls(WEBHOOK)).isEqualTo(1);
    }

    private static List<String> readTopic(String topic) {
        var values = new ArrayList<String>();
        try (var consumer = new KafkaConsumer<String, String>(Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "reader-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(300)).forEach(r -> values.add(r.value()));
                if (!values.isEmpty()) {
                    consumer.poll(Duration.ofMillis(300)).forEach(r -> values.add(r.value()));
                    break;
                }
            }
        }
        return values;
    }
}
