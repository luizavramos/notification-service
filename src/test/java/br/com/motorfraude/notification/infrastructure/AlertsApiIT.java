package br.com.motorfraude.notification.infrastructure;

import static br.com.motorfraude.notification.TestAlerts.ACCOUNT;
import static br.com.motorfraude.notification.TestAlerts.NOW;
import static br.com.motorfraude.notification.TestAlerts.alert;
import static br.com.motorfraude.notification.TestAlerts.id;
import static org.assertj.core.api.Assertions.assertThat;

import br.com.motorfraude.contracts.Severity;
import br.com.motorfraude.notification.application.gateway.AlertRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.web.client.RestClient;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class AlertsApiIT extends AbstractIntegrationTest {

    @LocalServerPort
    int port;

    @Autowired
    AlertRepository alerts;

    @Autowired
    JdbcClient jdbc;

    RestClient http;

    @BeforeEach
    void seed() {
        http = RestClient.builder().baseUrl("http://localhost:" + port).defaultStatusHandler(s -> true, (rq, rs) -> { }).build();
        jdbc.sql("TRUNCATE alerts, notification_delivery, alert_feedback").update();
        for (int i = 1; i <= 7; i++) {
            alerts.insertIfAbsent(alert(id(i), "tx-api-%04d-abcd".formatted(i), i % 2 == 0 ? Severity.HIGH : Severity.MEDIUM, NOW.plusSeconds(i)));
        }
    }

    private Map<String, Object> get(String uri) {
        return http.get().uri(uri).retrieve().body(new ParameterizedTypeReference<>() { });
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> items(Map<String, Object> body) {
        return (List<Map<String, Object>>) body.get("items");
    }

    @Test
    @DisplayName("H6.1: lista paginada por cursor, do mais recente ao mais antigo, sem repetir e sem pular")
    void paginacao() {
        var ids = new ArrayList<Object>();
        String cursor = null;
        int pages = 0;
        do {
            var body = get("/v1/alerts?limit=3" + (cursor == null ? "" : "&cursor=" + cursor));
            items(body).forEach(i -> ids.add(i.get("alertId")));
            cursor = (String) body.get("nextCursor");
            pages++;
        } while (cursor != null);

        assertThat(ids).hasSize(7).doesNotHaveDuplicates().startsWith(id(7), id(6));
        assertThat(pages).isEqualTo(3);
    }

    @Test
    @DisplayName("H6.1: filtros accountId, severity e from")
    void filtros() {
        assertThat(items(get("/v1/alerts?accountId=" + ACCOUNT + "&severity=HIGH"))).hasSize(3);
        assertThat(items(get("/v1/alerts?accountId=outra-conta-0001"))).isEmpty();
        assertThat(items(get("/v1/alerts?from=" + NOW.plusSeconds(5)))).hasSize(3);
        var first = items(get("/v1/alerts?limit=1")).getFirst();
        assertThat(first).containsKeys("alertId", "transactionId", "accountId", "severity", "score", "detectedAt", "ruleIds");
        assertThat(first).doesNotContainKey("payload");
    }

    @Test
    @DisplayName("parâmetros inválidos dão 400 em Problem Details")
    void invalidos() {
        for (var uri : List.of("/v1/alerts?severity=LOW", "/v1/alerts?limit=0", "/v1/alerts?limit=999", "/v1/alerts?cursor=lixo", "/v1/alerts?from=ontem")) {
            var response = http.get().uri(uri).retrieve().toEntity(String.class);
            assertThat(response.getStatusCode()).as(uri).isEqualTo(HttpStatus.BAD_REQUEST);
            assertThat(response.getHeaders().getContentType().isCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON)).as(uri).isTrue();
        }
    }

    @Test
    @DisplayName("H6.2: feedback é gravado (201) e a métrica de falso positivo por regra sobe")
    void feedback() {
        var response = http.post().uri("/v1/alerts/{id}/feedback", id(2)).header("X-Analyst", "ana").contentType(MediaType.APPLICATION_JSON)
                .body("{\"verdict\":\"FALSE_POSITIVE\"}").retrieve().toEntity(String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(jdbc.sql("SELECT verdict || ':' || analyst FROM alert_feedback WHERE alert_id = :a").param("a", id(2)).query(String.class).single())
                .isEqualTo("FALSE_POSITIVE:ana");
        var prometheus = http.get().uri("/actuator/prometheus").retrieve().body(String.class);
        assertThat(prometheus).containsPattern("alerts_feedback_total\\{[^}]*rule=\"R10\"[^}]*verdict=\"FALSE_POSITIVE\"[^}]*\\} 1\\.0|"
                + "alerts_feedback_total\\{[^}]*verdict=\"FALSE_POSITIVE\"[^}]*rule=\"R10\"[^}]*\\} 1\\.0");
    }

    @Test
    @DisplayName("feedback: alerta inexistente é 404, veredito desconhecido ou corpo vazio é 400, e nada é gravado")
    void feedbackInvalido() {
        var missing = http.post().uri("/v1/alerts/{id}/feedback", id(99)).contentType(MediaType.APPLICATION_JSON)
                .body("{\"verdict\":\"CONFIRMED_FRAUD\"}").retrieve().toEntity(String.class);
        var unknown = http.post().uri("/v1/alerts/{id}/feedback", id(1)).contentType(MediaType.APPLICATION_JSON)
                .body("{\"verdict\":\"TALVEZ\"}").retrieve().toEntity(String.class);
        var empty = http.post().uri("/v1/alerts/{id}/feedback", id(1)).contentType(MediaType.APPLICATION_JSON).body("{}").retrieve()
                .toEntity(String.class);

        assertThat(missing.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(unknown.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(empty.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(jdbc.sql("SELECT count(*) FROM alert_feedback").query(Long.class).single()).isZero();
    }
}
