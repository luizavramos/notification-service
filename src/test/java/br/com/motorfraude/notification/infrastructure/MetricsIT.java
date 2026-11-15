package br.com.motorfraude.notification.infrastructure;

import static br.com.motorfraude.notification.TestAlerts.ACCOUNT;
import static br.com.motorfraude.notification.TestAlerts.alert;
import static br.com.motorfraude.notification.TestAlerts.id;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;

import br.com.motorfraude.contracts.JsonCodec;
import br.com.motorfraude.contracts.Severity;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.web.client.RestClient;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@org.springframework.test.annotation.DirtiesContext(classMode = org.springframework.test.annotation.DirtiesContext.ClassMode.BEFORE_CLASS)
class MetricsIT extends AbstractIntegrationTest {

    @LocalServerPort
    int port;

    @org.springframework.beans.factory.annotation.Autowired
    org.springframework.jdbc.core.simple.JdbcClient jdbc;

    @BeforeEach
    void stubs() {
        jdbc.sql("TRUNCATE alerts, notification_delivery, alert_feedback").update();
        PROVIDERS.resetAll();
        PROVIDERS.stubFor(post(urlEqualTo("/antifraud-legacy/webhooks/fraud-alerts")).willReturn(aResponse().withStatus(202).withBody("{\"caseId\":\"C\"}")));
        PROVIDERS.stubFor(post(urlEqualTo("/push-provider/v1/push")).willReturn(aResponse().withStatus(503)));
        PROVIDERS.stubFor(post(urlEqualTo("/email-provider/v1/email")).willReturn(aResponse().withStatus(202).withBody("{\"messageId\":\"e\"}")));
        PROVIDERS.stubFor(post(urlEqualTo("/sms-provider/v1/sms")).willReturn(aResponse().withStatus(202).withBody("{\"messageId\":\"s\"}")));
        PROVIDERS.stubFor(get(urlEqualTo("/contacts/v1/contacts/" + ACCOUNT)).willReturn(aResponse().withStatus(200)
                .withBody("{\"pushToken\":\"t\",\"email\":\"c@exemplo.invalid\",\"phone\":\"+5511900000000\"}")));
    }

    private String scrape() {
        return RestClient.create("http://localhost:" + port).get().uri("/actuator/prometheus").retrieve().body(String.class);
    }

    private static void await(String what, BooleanSupplier condition) {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
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
        throw new AssertionError("não aconteceu em 60 s: " + what);
    }

    @Test
    @DisplayName("as séries nascem zeradas, para o painel não ter buraco antes do primeiro alerta")
    void nascemZeradas() {
        var text = scrape();

        for (var channel : new String[] {"ANTIFRAUD_WEBHOOK", "PUSH", "EMAIL", "SMS"}) {
            assertThat(text).contains("notification_deliveries_total{channel=\"" + channel + "\",result=\"sent\"");
            assertThat(text).contains("notification_dlq_total{channel=\"" + channel + "\"");
            assertThat(text).containsPattern("resilience4j_circuitbreaker_state\\{name=\"" + channel + "\"[^}]*state=\"closed\"[^}]*\\} 1\\.0");
        }
        assertThat(text).contains("notification_dlq_total{channel=\"INVALID_ALERT\"").contains("notification_pending_reconciled_total");
    }

    @Test
    @DisplayName("um HIGH com o push em 503 aparece nas métricas: enviados, retry, falha, DLQ, SMS de reserva e latência alerta→entrega")
    void entregasAparecem() {
        var detected = Instant.now();
        var alert = alert(id(1), "tx-metricas-0001", Severity.HIGH, detected);
        try (var producer = new KafkaProducer<String, String>(Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class))) {
            producer.send(new ProducerRecord<>("fraud-alerts.v1", alert.accountId(), JsonCodec.toJson(alert)));
            producer.flush();
        }

        await("SMS de reserva enviado", () -> scrape().matches("(?s).*notification_deliveries_total\\{channel=\"SMS\",result=\"sent\"[^}]*\\} 1\\.0.*"));

        var text = scrape();
        assertThat(text).containsPattern("notification_deliveries_total\\{channel=\"ANTIFRAUD_WEBHOOK\",result=\"sent\"[^}]*\\} 1\\.0");
        assertThat(text).containsPattern("notification_deliveries_total\\{channel=\"EMAIL\",result=\"sent\"[^}]*\\} 1\\.0");
        assertThat(text).containsPattern("notification_deliveries_total\\{channel=\"PUSH\",result=\"retry\"[^}]*\\} 3\\.0");
        assertThat(text).containsPattern("notification_deliveries_total\\{channel=\"PUSH\",result=\"failed\"[^}]*\\} 1\\.0");
        assertThat(text).containsPattern("notification_dlq_total\\{channel=\"PUSH\"[^}]*\\} 1\\.0");
        assertThat(text).containsPattern("notification_alert_to_delivery_seconds_count\\{channel=\"ANTIFRAUD_WEBHOOK\"[^}]*\\} 1");
        assertThat(text).containsPattern("notification_alert_to_delivery_seconds_bucket\\{channel=\"ANTIFRAUD_WEBHOOK\"[^}]*le=\"1\\.0\"");
    }
}
