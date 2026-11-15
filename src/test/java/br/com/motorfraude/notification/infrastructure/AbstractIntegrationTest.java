package br.com.motorfraude.notification.infrastructure;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

import com.github.tomakehurst.wiremock.WireMockServer;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

abstract class AbstractIntegrationTest {

    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16.16");
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.1.0");
    static final WireMockServer PROVIDERS = new WireMockServer(wireMockConfig().dynamicPort());

    static {
        POSTGRES.start();
        KAFKA.start();
        PROVIDERS.start();
        try (var admin = AdminClient.create(Map.<String, Object>of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(
                    new NewTopic("fraud-alerts.v1", 1, (short) 1), new NewTopic("alerts.retry.1m", 1, (short) 1),
                    new NewTopic("alerts.retry.5m", 1, (short) 1), new NewTopic("alerts.retry.30m", 1, (short) 1),
                    new NewTopic("alerts.dlq", 1, (short) 1))).all().get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("notification.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("notification.kafka.failure-backoff", () -> "500ms");
        registry.add("notification.providers.antifraud.url", () -> PROVIDERS.baseUrl() + "/antifraud-legacy/webhooks/fraud-alerts");
        registry.add("notification.providers.antifraud.secret", () -> "segredo-do-teste");
        registry.add("notification.providers.push.url", () -> PROVIDERS.baseUrl() + "/push-provider/v1/push");
        registry.add("notification.providers.email.url", () -> PROVIDERS.baseUrl() + "/email-provider/v1/email");
        registry.add("notification.providers.sms.url", () -> PROVIDERS.baseUrl() + "/sms-provider/v1/sms");
        registry.add("notification.providers.contacts.url", () -> PROVIDERS.baseUrl() + "/contacts");
        registry.add("notification.retry.steps[0].delay", () -> "300ms");
        registry.add("notification.retry.steps[0].topic", () -> "alerts.retry.1m");
        registry.add("notification.retry.steps[1].delay", () -> "600ms");
        registry.add("notification.retry.steps[1].topic", () -> "alerts.retry.5m");
        registry.add("notification.retry.steps[2].delay", () -> "900ms");
        registry.add("notification.retry.steps[2].topic", () -> "alerts.retry.30m");
        registry.add("notification.resilience.max-attempts", () -> "1");
        registry.add("notification.resilience.minimum-calls", () -> "1000");
        registry.add("notification.resilience.initial-backoff", () -> "10ms");
        registry.add("notification.reconciler.interval", () -> "1h");
    }
}
