package br.com.motorfraude.notification.adapters.gateway.channels;

import static br.com.motorfraude.notification.TestAlerts.high;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;

import br.com.motorfraude.contracts.FraudAlert;
import br.com.motorfraude.contracts.JsonCodec;
import br.com.motorfraude.notification.application.gateway.NotificationChannel.Outbound;
import br.com.motorfraude.notification.application.gateway.NotificationChannel.Result.Status;
import br.com.motorfraude.notification.domain.Channel;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class AntifraudWebhookChannelTest {

    private static final String SECRET = "segredo-de-teste";
    private static final String PATH = "/antifraud-legacy/webhooks/fraud-alerts";

    @RegisterExtension
    static final WireMockExtension SERVER = WireMockExtension.newInstance().options(wireMockConfig().dynamicPort()).build();

    private AntifraudWebhookChannel channel(Duration timeout) {
        return new AntifraudWebhookChannel(HttpClient.newHttpClient(), URI.create(SERVER.baseUrl() + PATH), SECRET, timeout);
    }

    private static Outbound outbound(FraudAlert alert) {
        return new Outbound(alert, null, alert.alertId() + ":ANTIFRAUD_WEBHOOK");
    }

    private static String hmac(String body) throws Exception {
        var mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(SECRET.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        return java.util.HexFormat.of().formatHex(mac.doFinal(body.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    @DisplayName("H1: envia o alerta como JSON, com X-Signature (HMAC-SHA256 do corpo) e Idempotency-Key alertId:canal, e devolve o caso")
    void enviaAssinado() throws Exception {
        SERVER.stubFor(post(urlEqualTo(PATH)).willReturn(aResponse().withStatus(202).withHeader("Content-Type", "application/json")
                .withBody("{\"caseId\":\"CASE-123\"}")));
        var alert = high("a1");

        var result = channel(Duration.ofSeconds(2)).send(outbound(alert));

        assertThat(result.status()).isEqualTo(Status.SENT);
        assertThat(result.providerMessageId()).isEqualTo("CASE-123");
        var request = SERVER.findAll(postRequestedFor(urlEqualTo(PATH))).getFirst();
        assertThat(request.getHeader("Idempotency-Key")).isEqualTo("a1:ANTIFRAUD_WEBHOOK");
        assertThat(request.getHeader("Content-Type")).startsWith("application/json");
        assertThat(request.getHeader("X-Signature")).isEqualTo(hmac(request.getBodyAsString()));
        assertThat(JsonCodec.fromJson(request.getBodyAsString(), FraudAlert.class)).isEqualTo(alert);
    }

    @Test
    @DisplayName("a assinatura muda quando o segredo muda (quem não tem o segredo não consegue forjar)")
    void assinaturaDependeDoSegredo() throws Exception {
        SERVER.stubFor(post(urlEqualTo(PATH)).willReturn(aResponse().withStatus(202).withBody("{}")));
        var other = new AntifraudWebhookChannel(HttpClient.newHttpClient(), URI.create(SERVER.baseUrl() + PATH), "outro-segredo", Duration.ofSeconds(2));
        var alert = high("a1");

        other.send(outbound(alert));

        var request = SERVER.findAll(postRequestedFor(urlEqualTo(PATH))).getFirst();
        assertThat(request.getHeader("X-Signature")).isNotEqualTo(hmac(request.getBodyAsString())).matches("[0-9a-f]{64}");
    }

    @Test
    @DisplayName("resposta 2xx sem corpo útil ainda é sucesso")
    void semCorpo() {
        SERVER.stubFor(post(urlEqualTo(PATH)).willReturn(aResponse().withStatus(200)));

        var result = channel(Duration.ofSeconds(2)).send(outbound(high("a1")));

        assertThat(result.status()).isEqualTo(Status.SENT);
        assertThat(result.providerMessageId()).isNotBlank();
    }

    @Test
    @DisplayName("503, 500, 429 e 408 são falhas com nova tentativa")
    void falhasTransitorias() {
        for (int status : new int[] {503, 500, 429, 408}) {
            SERVER.stubFor(post(urlEqualTo(PATH)).willReturn(aResponse().withStatus(status)));

            var result = channel(Duration.ofSeconds(2)).send(outbound(high("a1")));

            assertThat(result.status()).as("HTTP %d", status).isEqualTo(Status.RETRYABLE_FAILURE);
            assertThat(result.error()).contains(String.valueOf(status));
        }
    }

    @Test
    @DisplayName("400, 401 e 422 são recusas definitivas, sem nova tentativa")
    void recusaDefinitiva() {
        for (int status : new int[] {400, 401, 403, 422}) {
            SERVER.stubFor(post(urlEqualTo(PATH)).willReturn(aResponse().withStatus(status)));

            var result = channel(Duration.ofSeconds(2)).send(outbound(high("a1")));

            assertThat(result.status()).as("HTTP %d", status).isEqualTo(Status.PERMANENT_FAILURE);
        }
    }

    @Test
    @DisplayName("provedor mais lento que o timeout: falha com nova tentativa, sem esperar a resposta")
    void timeout() {
        SERVER.stubFor(post(urlEqualTo(PATH)).willReturn(aResponse().withStatus(202).withFixedDelay(3_000)));
        long start = System.nanoTime();

        var result = channel(Duration.ofMillis(400)).send(outbound(high("a1")));

        assertThat(result.status()).isEqualTo(Status.RETRYABLE_FAILURE);
        assertThat(result.error()).containsIgnoringCase("timeout");
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(2));
    }

    @Test
    @DisplayName("provedor fora do ar (conexão recusada): falha com nova tentativa")
    void conexaoRecusada() {
        var down = new AntifraudWebhookChannel(HttpClient.newHttpClient(), URI.create("http://localhost:1/webhooks"), SECRET, Duration.ofSeconds(1));

        var result = down.send(outbound(high("a1")));

        assertThat(result.status()).isEqualTo(Status.RETRYABLE_FAILURE);
    }

    @Test
    @DisplayName("o canal se identifica como ANTIFRAUD_WEBHOOK")
    void identifica() {
        assertThat(channel(Duration.ofSeconds(1)).channel()).isEqualTo(Channel.ANTIFRAUD_WEBHOOK);
        assertThat(equalTo("x")).isNotNull();
    }
}
