package br.com.motorfraude.notification.adapters.controller.kafka;

import static br.com.motorfraude.notification.TestAlerts.ACCOUNT;
import static br.com.motorfraude.notification.TestAlerts.NOW;
import static br.com.motorfraude.notification.TestAlerts.high;
import static br.com.motorfraude.notification.TestAlerts.medium;
import static org.assertj.core.api.Assertions.assertThat;

import br.com.motorfraude.contracts.FraudAlert;
import br.com.motorfraude.contracts.JsonCodec;
import br.com.motorfraude.notification.adapters.gateway.kafka.KafkaRetryScheduler;
import br.com.motorfraude.notification.application.gateway.AlertRepository;
import br.com.motorfraude.notification.application.gateway.Contact;
import br.com.motorfraude.notification.application.usecase.DeliveryDispatcher;
import br.com.motorfraude.notification.application.usecase.DispatchDeliveryUseCase;
import br.com.motorfraude.notification.application.usecase.RegisterAlertUseCase;
import br.com.motorfraude.notification.domain.Channel;
import br.com.motorfraude.notification.domain.DeliveryStatus;
import br.com.motorfraude.notification.domain.SuppressionPolicy;
import br.com.motorfraude.notification.fakes.DirectUnitOfWork;
import br.com.motorfraude.notification.fakes.InMemoryAlertRepository;
import br.com.motorfraude.notification.fakes.InMemoryContactDirectory;
import br.com.motorfraude.notification.fakes.InMemoryDeliveryRepository;
import br.com.motorfraude.notification.fakes.MutableClock;
import br.com.motorfraude.notification.fakes.RecordingRetryScheduler;
import br.com.motorfraude.notification.fakes.ScriptedChannel;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class AlertConsumerLoopTest {

    private static final String TOPIC = "fraud-alerts.v1";
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);
    private static final String A1 = br.com.motorfraude.notification.TestAlerts.id(1);
    private static final String A2 = br.com.motorfraude.notification.TestAlerts.id(2);

    private final InMemoryAlertRepository alertStore = new InMemoryAlertRepository();
    private final InMemoryDeliveryRepository deliveries = new InMemoryDeliveryRepository(alertStore);
    private final InMemoryContactDirectory contacts = new InMemoryContactDirectory();
    private final ScriptedChannel webhook = new ScriptedChannel(Channel.ANTIFRAUD_WEBHOOK);
    private final ScriptedChannel push = new ScriptedChannel(Channel.PUSH);
    private final ScriptedChannel email = new ScriptedChannel(Channel.EMAIL);
    private final MutableClock clock = new MutableClock(NOW);
    private final List<Runnable> queued = new ArrayList<>();
    private final MockProducer<String, String> dlqProducer = new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
    private final List<String> alertsAtCommit = new ArrayList<>();
    private boolean databaseDown;
    private MockConsumer<String, String> consumer;
    private AlertConsumerLoop loop;
    private long offset;

    @BeforeEach
    void setUp() {
        consumer = new MockConsumer<>("earliest") {
            @Override
            public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
                alertStore.all().forEach(a -> alertsAtCommit.add(a.alertId()));
                super.commitSync(offsets);
            }
        };
        contacts.put(ACCOUNT, new Contact("push-token", "cliente@exemplo.invalid", "+5511900000000"));
        AlertRepository repository = new AlertRepository() {
            @Override
            public boolean insertIfAbsent(FraudAlert alert) {
                if (databaseDown) {
                    throw new IllegalStateException("banco fora do ar");
                }
                return alertStore.insertIfAbsent(alert);
            }

            @Override
            public Optional<FraudAlert> find(String alertId) {
                return alertStore.find(alertId);
            }
        };
        var register = new RegisterAlertUseCase(repository, deliveries, SuppressionPolicy.defaultPolicy(), new DirectUnitOfWork(), clock);
        var dispatch = new DispatchDeliveryUseCase(alertStore, deliveries,
                Map.of(Channel.ANTIFRAUD_WEBHOOK, webhook, Channel.PUSH, push, Channel.EMAIL, email), contacts, new RecordingRetryScheduler(), clock);
        var dispatcher = new DeliveryDispatcher(dispatch);
        var topics = new LinkedHashMap<Duration, String>();
        topics.put(Duration.ofMinutes(1), "alerts.retry.1m");
        var dlq = new KafkaRetryScheduler(dlqProducer, clock, topics, "alerts.dlq");
        loop = new AlertConsumerLoop(consumer, TOPIC, register, dispatcher::dispatch, queued::add, dlq, clock, Duration.ofSeconds(5));
        loop.initialize();
        consumer.rebalance(List.of(PARTITION));
        consumer.updateBeginningOffsets(Map.of(PARTITION, 0L));
    }

    private void publish(FraudAlert alert) {
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, offset++, alert.accountId(), JsonCodec.toJson(alert)));
    }

    private void runQueued() {
        var copy = List.copyOf(queued);
        queued.clear();
        copy.forEach(Runnable::run);
    }

    private long committed() {
        var committed = consumer.committed(Set.of(PARTITION)).get(PARTITION);
        return committed == null ? -1 : committed.offset();
    }

    @Test
    @DisplayName("H1: alerta MEDIUM é gravado, o offset é confirmado e o webhook do antifraude é despachado")
    void alertaMedium() {
        publish(medium(A1));

        loop.pollOnce(Duration.ZERO);
        runQueued();

        assertThat(alertStore.find(A1)).isPresent();
        assertThat(committed()).isEqualTo(1);
        assertThat(webhook.calls()).hasSize(1);
        assertThat(push.calls()).isEmpty();
    }

    @Test
    @DisplayName("H2: alerta HIGH despacha o antifraude, o push e o e-mail")
    void alertaHigh() {
        publish(high(A1));

        loop.pollOnce(Duration.ZERO);
        runQueued();

        assertThat(webhook.calls()).hasSize(1);
        assertThat(push.calls()).hasSize(1);
        assertThat(email.calls()).hasSize(1);
    }

    @Test
    @DisplayName("FR-001: o offset só é confirmado depois do alerta gravado, e o envio vem depois da confirmação (o despacho é assíncrono)")
    void confirmaDepoisDeGravarEAntesDeEnviar() {
        publish(high(A1));

        loop.pollOnce(Duration.ZERO);

        assertThat(alertsAtCommit).contains(A1);
        assertThat(committed()).isEqualTo(1);
        assertThat(webhook.calls()).as("nada foi enviado antes do executor rodar").isEmpty();
        assertThat(queued).hasSize(3);
    }

    @Test
    @DisplayName("H3: o mesmo alerta de novo (reentrega) não reenvia o que já saiu")
    void reentrega() {
        publish(high(A1));
        loop.pollOnce(Duration.ZERO);
        runQueued();

        publish(high(A1));
        loop.pollOnce(Duration.ZERO);
        runQueued();

        assertThat(webhook.calls()).hasSize(1);
        assertThat(push.calls()).hasSize(1);
        assertThat(email.calls()).hasSize(1);
        assertThat(deliveries.find(A1, Channel.PUSH).orElseThrow().status()).isEqualTo(DeliveryStatus.SENT);
    }

    @Test
    @DisplayName("borda: mensagem que não é um alerta válido vai para a DLQ com o motivo, o offset é confirmado e a partição segue")
    void invalida() {
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, offset++, ACCOUNT, "{não é json"));
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, offset++, ACCOUNT, "{\"alertId\":\"x\"}"));
        publish(medium(A2));

        loop.pollOnce(Duration.ZERO);
        runQueued();

        assertThat(dlqProducer.history()).hasSize(2);
        assertThat(dlqProducer.history()).allSatisfy(r -> assertThat(r.topic()).isEqualTo("alerts.dlq"));
        assertThat(new String(dlqProducer.history().getFirst().headers().lastHeader("dlq.reason").value(), StandardCharsets.UTF_8)).isNotBlank();
        assertThat(alertStore.find(A2)).isPresent();
        assertThat(committed()).isEqualTo(3);
    }

    @Test
    @DisplayName("borda: banco de alertas fora do ar: não confirma o offset, não perde nada, pausa e tenta de novo depois do intervalo")
    void bancoForaDoAr() {
        publish(medium(A1));
        databaseDown = true;

        loop.pollOnce(Duration.ZERO);

        assertThat(committed()).isEqualTo(-1);
        assertThat(queued).isEmpty();
        assertThat(consumer.paused()).containsExactly(PARTITION);
        assertThat(consumer.position(PARTITION)).isEqualTo(0);

        databaseDown = false;
        publish(medium(A1));
        clock.advance(Duration.ofSeconds(6));
        loop.pollOnce(Duration.ZERO);
        loop.pollOnce(Duration.ZERO);
        runQueued();

        assertThat(consumer.paused()).isEmpty();
        assertThat(alertStore.find(A1)).isPresent();
    }

    @Test
    @DisplayName("falha inesperada no despacho não impede a confirmação nem os outros alertas (o reconciliador recupera)")
    void despachoFalhando() {
        var loopWithBrokenDispatch = new AlertConsumerLoop(consumer, TOPIC,
                new RegisterAlertUseCase(alertStore, deliveries, SuppressionPolicy.defaultPolicy(), new DirectUnitOfWork(), clock),
                (alertId, channel) -> {
                    throw new IllegalStateException("provedor explodiu");
                }, Runnable::run, new KafkaRetryScheduler(dlqProducer, clock, Map.of(Duration.ofMinutes(1), "alerts.retry.1m"), "alerts.dlq"), clock,
                Duration.ofSeconds(5));
        loopWithBrokenDispatch.initialize();
        consumer.rebalance(List.of(PARTITION));
        consumer.updateBeginningOffsets(Map.of(PARTITION, 0L));
        publish(medium(A1));
        publish(medium(A2));

        loopWithBrokenDispatch.pollOnce(Duration.ZERO);

        assertThat(alertStore.find(A1)).isPresent();
        assertThat(alertStore.find(A2)).isPresent();
        assertThat(committed()).isEqualTo(2);
    }

    @Test
    @DisplayName("falha do Kafka ao ler não derruba o laço")
    void kafkaFalha() {
        consumer.setPollException(new KafkaException("broker fora do ar"));

        loop.pollOnce(Duration.ZERO);
        publish(medium(A1));
        loop.pollOnce(Duration.ZERO);

        assertThat(alertStore.find(A1)).isPresent();
    }
}
