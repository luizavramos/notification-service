package br.com.motorfraude.notification.infrastructure;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class NotificationStartupIT extends AbstractIntegrationTest {

    @Autowired
    JdbcTemplate jdbc;

    @Value("${local.server.port}")
    int port;

    @Autowired
    Environment env;

    private final HttpClient http = HttpClient.newHttpClient();

    private HttpResponse<String> get(String path) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).GET().build();
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }

    @Test
    @DisplayName("o serviço sobe, está vivo e pronto (200)")
    void saude() throws Exception {
        assertThat(get("/actuator/health/liveness").statusCode()).isEqualTo(200);
        assertThat(get("/actuator/health/readiness").statusCode()).isEqualTo(200);
    }

    @Test
    @DisplayName("/actuator/prometheus expõe métricas da JVM com o rótulo service")
    void metricas() throws Exception {
        var response = get("/actuator/prometheus");

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("jvm_memory_used_bytes").contains("service=\"notification-service\"");
    }

    @Test
    @DisplayName("as migrações do Flyway rodam na subida e criam as tabelas do plano")
    void migracoes() {
        assertThat(jdbc.queryForObject("select count(*) from flyway_schema_history where success", Long.class)).isGreaterThanOrEqualTo(1);
        assertThat(jdbc.queryForList("select tablename from pg_tables where schemaname = 'public'", String.class))
                .contains("alerts", "notification_delivery", "alert_feedback");
    }

    @Test
    @DisplayName("application.yml: porta 8085 (o teste usa porta aleatória) e virtual threads (plan §2)")
    void configuracaoPadrao() {
        var yaml = new YamlPropertiesFactoryBean();
        yaml.setResources(new ClassPathResource("application.yml"));
        var props = yaml.getObject();

        assertThat(props).isNotNull();
        assertThat(props.getProperty("server.port")).isEqualTo("8085");
        assertThat(props.getProperty("spring.threads.virtual.enabled")).isEqualTo("true");
        assertThat(env.getProperty("spring.threads.virtual.enabled", Boolean.class)).isTrue();
    }
}
