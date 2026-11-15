package br.com.motorfraude.notification.adapters.gateway.channels;

import static br.com.motorfraude.notification.TestAlerts.high;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;

import br.com.motorfraude.notification.application.gateway.Contact;
import br.com.motorfraude.notification.application.gateway.NotificationChannel;
import br.com.motorfraude.notification.application.gateway.NotificationChannel.Outbound;
import br.com.motorfraude.notification.application.gateway.NotificationChannel.Result;
import br.com.motorfraude.notification.application.gateway.NotificationChannel.Result.Status;
import br.com.motorfraude.notification.domain.Channel;
import br.com.motorfraude.notification.fakes.ScriptedChannel;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class ResilientChannelTest {

    @RegisterExtension
    static final WireMockExtension SERVER = WireMockExtension.newInstance().options(wireMockConfig().dynamicPort()).build();

    private static final Contact CONTACT = new Contact("push-1", "a@exemplo.invalid", "+5511900000000");

    private static ResilienceSettings settings(int attempts, int minimumCalls, Duration openWait) {
        return new ResilienceSettings(attempts, Duration.ofMillis(5), 2.0, 0.5, 10, minimumCalls, 50f, openWait, 2, 10, Duration.ZERO);
    }

    private static Outbound outbound(Channel channel) {
        var alert = high("a1");
        return new Outbound(alert, CONTACT, alert.alertId() + ":" + channel);
    }

    @BeforeEach
    void reset() {
        SERVER.resetAll();
    }

    @Test
    @DisplayName("sucesso de primeira: uma só chamada ao provedor")
    void sucesso() {
        var delegate = new ScriptedChannel(Channel.PUSH);
        var channel = new ResilientChannel(delegate, settings(3, 5, Duration.ofSeconds(30)));

        var result = channel.send(outbound(Channel.PUSH));

        assertThat(result.status()).isEqualTo(Status.SENT);
        assertThat(delegate.calls()).hasSize(1);
    }

    @Test
    @DisplayName("FR-003: falha transitória é repetida na hora; a segunda tentativa que dá certo vira SENT")
    void retryImediato() {
        var delegate = new ScriptedChannel(Channel.PUSH).then(Result.retryable("503")).then(Result.retryable("503")).then(Result.sent("m-3"));
        var channel = new ResilientChannel(delegate, settings(3, 5, Duration.ofSeconds(30)));

        var result = channel.send(outbound(Channel.PUSH));

        assertThat(result.status()).isEqualTo(Status.SENT);
        assertThat(result.providerMessageId()).isEqualTo("m-3");
        assertThat(delegate.calls()).hasSize(3);
    }

    @Test
    @DisplayName("3 tentativas imediatas falhando: devolve falha com nova tentativa, com o último erro, sem chamar uma 4ª vez")
    void esgotaOsRetriesImediatos() {
        var delegate = new ScriptedChannel(Channel.PUSH).alwaysFails("503 do provedor");
        var channel = new ResilientChannel(delegate, settings(3, 50, Duration.ofSeconds(30)));

        var result = channel.send(outbound(Channel.PUSH));

        assertThat(result.status()).isEqualTo(Status.RETRYABLE_FAILURE);
        assertThat(result.error()).contains("503 do provedor");
        assertThat(delegate.calls()).hasSize(3);
    }

    @Test
    @DisplayName("recusa definitiva não é repetida")
    void definitivaNaoRepete() {
        var delegate = new ScriptedChannel(Channel.PUSH).then(Result.permanent("HTTP 400"));
        var channel = new ResilientChannel(delegate, settings(3, 5, Duration.ofSeconds(30)));

        var result = channel.send(outbound(Channel.PUSH));

        assertThat(result.status()).isEqualTo(Status.PERMANENT_FAILURE);
        assertThat(delegate.calls()).hasSize(1);
    }

    @Test
    @DisplayName("o intervalo entre as tentativas imediatas cresce de forma exponencial, com jitter")
    void backoffComJitter() {
        var times = Collections.synchronizedList(new ArrayList<Long>());
        var delegate = new NotificationChannel() {
            @Override
            public Channel channel() {
                return Channel.PUSH;
            }

            @Override
            public Result send(Outbound outbound) {
                times.add(System.nanoTime());
                return Result.retryable("503");
            }
        };
        var withBackoff = new ResilienceSettings(3, Duration.ofMillis(100), 2.0, 0.5, 10, 50, 50f, Duration.ofSeconds(30), 2, 10, Duration.ZERO);

        new ResilientChannel(delegate, withBackoff).send(outbound(Channel.PUSH));

        long firstGap = Duration.ofNanos(times.get(1) - times.get(0)).toMillis();
        long secondGap = Duration.ofNanos(times.get(2) - times.get(1)).toMillis();
        assertThat(firstGap).as("1º intervalo (100 ms ± 50%)").isBetween(45L, 400L);
        assertThat(secondGap).as("2º intervalo (200 ms ± 50%)").isBetween(95L, 600L);
    }

    @Test
    @DisplayName("H4: com o provedor em 503, o circuit breaker abre e para de chamar; depois do tempo, fecha quando o provedor volta")
    void circuitBreakerAbreEFecha() throws Exception {
        var path = "/push-provider/v1/push";
        SERVER.stubFor(post(urlEqualTo(path)).willReturn(aResponse().withStatus(503)));
        var push = new PushChannel(HttpClient.newHttpClient(), URI.create(SERVER.baseUrl() + path), Duration.ofSeconds(1));
        var channel = new ResilientChannel(push, settings(1, 5, Duration.ofMillis(400)));

        for (int i = 0; i < 5; i++) {
            assertThat(channel.send(outbound(Channel.PUSH)).status()).isEqualTo(Status.RETRYABLE_FAILURE);
        }
        assertThat(channel.circuitState()).isEqualTo(CircuitBreaker.State.OPEN);

        var rejected = channel.send(outbound(Channel.PUSH));
        assertThat(rejected.status()).isEqualTo(Status.RETRYABLE_FAILURE);
        assertThat(rejected.error()).containsIgnoringCase("circuit");
        SERVER.verify(5, postRequestedFor(urlEqualTo(path)));

        SERVER.stubFor(post(urlEqualTo(path)).willReturn(aResponse().withStatus(202).withBody("{\"messageId\":\"ok\"}")));
        Thread.sleep(600);
        assertThat(channel.send(outbound(Channel.PUSH)).status()).isEqualTo(Status.SENT);
        assertThat(channel.send(outbound(Channel.PUSH)).status()).isEqualTo(Status.SENT);

        assertThat(channel.circuitState()).isEqualTo(CircuitBreaker.State.CLOSED);
    }

    @Test
    @DisplayName("recusas definitivas (HTTP 400) não contam como falha do provedor: o circuito continua fechado")
    void definitivasNaoAbremOCircuito() {
        var path = "/push-provider/v1/push";
        SERVER.stubFor(post(urlEqualTo(path)).willReturn(aResponse().withStatus(400)));
        var push = new PushChannel(HttpClient.newHttpClient(), URI.create(SERVER.baseUrl() + path), Duration.ofSeconds(1));
        var channel = new ResilientChannel(push, settings(1, 5, Duration.ofSeconds(30)));

        for (int i = 0; i < 12; i++) {
            assertThat(channel.send(outbound(Channel.PUSH)).status()).isEqualTo(Status.PERMANENT_FAILURE);
        }

        assertThat(channel.circuitState()).isEqualTo(CircuitBreaker.State.CLOSED);
    }

    @Test
    @DisplayName("H4.1: o circuito do push aberto não afeta o e-mail: cada canal tem o seu")
    void isolamentoEntreCanais() {
        SERVER.stubFor(post(urlEqualTo("/push")).willReturn(aResponse().withStatus(503)));
        SERVER.stubFor(post(urlEqualTo("/email")).willReturn(aResponse().withStatus(202).withBody("{\"messageId\":\"e-1\"}")));
        var http = HttpClient.newHttpClient();
        var push = new ResilientChannel(new PushChannel(http, URI.create(SERVER.baseUrl() + "/push"), Duration.ofSeconds(1)),
                settings(1, 5, Duration.ofSeconds(30)));
        var email = new ResilientChannel(new EmailChannel(http, URI.create(SERVER.baseUrl() + "/email"), Duration.ofSeconds(1)),
                settings(1, 5, Duration.ofSeconds(30)));

        for (int i = 0; i < 6; i++) {
            push.send(outbound(Channel.PUSH));
        }

        assertThat(push.circuitState()).isEqualTo(CircuitBreaker.State.OPEN);
        assertThat(email.circuitState()).isEqualTo(CircuitBreaker.State.CLOSED);
        assertThat(email.send(outbound(Channel.EMAIL)).status()).isEqualTo(Status.SENT);
    }

    @Test
    @DisplayName("o bulkhead limita as chamadas simultâneas ao provedor; as que passam do limite voltam como falha com nova tentativa")
    void bulkhead() throws Exception {
        var inside = new CountDownLatch(2);
        var release = new CountDownLatch(1);
        var delegate = new NotificationChannel() {
            @Override
            public Channel channel() {
                return Channel.PUSH;
            }

            @Override
            public Result send(Outbound outbound) {
                inside.countDown();
                try {
                    release.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return Result.sent("m");
            }
        };
        var limited = new ResilienceSettings(1, Duration.ofMillis(5), 2.0, 0.5, 10, 50, 50f, Duration.ofSeconds(30), 2, 2, Duration.ZERO);
        var channel = new ResilientChannel(delegate, limited);

        try (var executor = Executors.newFixedThreadPool(5)) {
            List<Future<Result>> futures = new ArrayList<>();
            futures.add(executor.submit(() -> channel.send(outbound(Channel.PUSH))));
            futures.add(executor.submit(() -> channel.send(outbound(Channel.PUSH))));
            assertThat(inside.await(5, TimeUnit.SECONDS)).isTrue();
            for (int i = 0; i < 3; i++) {
                var overflow = executor.submit(() -> channel.send(outbound(Channel.PUSH))).get(5, TimeUnit.SECONDS);
                assertThat(overflow.status()).isEqualTo(Status.RETRYABLE_FAILURE);
                assertThat(overflow.error()).containsIgnoringCase("bulkhead");
            }
            release.countDown();
            for (var future : futures) {
                assertThat(future.get(5, TimeUnit.SECONDS).status()).isEqualTo(Status.SENT);
            }
        }
    }

    @Test
    @DisplayName("o canal resiliente se identifica como o canal embrulhado")
    void identifica() {
        var channel = new ResilientChannel(new ScriptedChannel(Channel.SMS), settings(1, 5, Duration.ofSeconds(1)));

        assertThat(channel.channel()).isEqualTo(Channel.SMS);
    }
}
