package br.com.motorfraude.notification.adapters.gateway.channels;

import static br.com.motorfraude.notification.TestAlerts.high;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.anyRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;

import br.com.motorfraude.contracts.JsonCodec;
import br.com.motorfraude.notification.TestAlerts;
import br.com.motorfraude.notification.application.gateway.Contact;
import br.com.motorfraude.notification.application.gateway.NotificationChannel;
import br.com.motorfraude.notification.application.gateway.NotificationChannel.Outbound;
import br.com.motorfraude.notification.application.gateway.NotificationChannel.Result.Status;
import br.com.motorfraude.notification.domain.Channel;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class CustomerChannelsTest {

    @RegisterExtension
    static final WireMockExtension SERVER = WireMockExtension.newInstance().options(wireMockConfig().dynamicPort()).build();

    private static final Contact CONTACT = new Contact("push-token-123", "cliente@exemplo.invalid", "+5511900000000");

    private record Case(String name, String path, NotificationChannel channel, Channel kind, String recipientField, String recipientValue) {
    }

    private static Case[] cases(Duration timeout) {
        var http = HttpClient.newHttpClient();
        return new Case[] {
                new Case("push", "/push-provider/v1/push", new PushChannel(http, URI.create(SERVER.baseUrl() + "/push-provider/v1/push"), timeout),
                        Channel.PUSH, "pushToken", "push-token-123"),
                new Case("e-mail", "/email-provider/v1/email",
                        new EmailChannel(http, URI.create(SERVER.baseUrl() + "/email-provider/v1/email"), timeout), Channel.EMAIL, "to",
                        "cliente@exemplo.invalid"),
                new Case("SMS", "/sms-provider/v1/sms", new SmsChannel(http, URI.create(SERVER.baseUrl() + "/sms-provider/v1/sms"), timeout),
                        Channel.SMS, "to", "+5511900000000")};
    }

    private static Outbound outbound(Case c, Contact contact) {
        var alert = high("a1");
        return new Outbound(alert, contact, alert.alertId() + ":" + c.kind().name());
    }

    @Test
    @DisplayName("H2: cada canal faz POST no seu caminho, com Idempotency-Key alertId:canal e o destinatário do contato")
    void enviaParaOProvedor() {
        for (var c : cases(Duration.ofSeconds(2))) {
            SERVER.resetAll();
            SERVER.stubFor(post(urlEqualTo(c.path())).willReturn(aResponse().withStatus(202).withHeader("Content-Type", "application/json")
                    .withBody("{\"messageId\":\"msg-" + c.name() + "\"}")));

            var result = c.channel().send(outbound(c, CONTACT));

            assertThat(result.status()).as(c.name()).isEqualTo(Status.SENT);
            assertThat(result.providerMessageId()).isEqualTo("msg-" + c.name());
            var request = SERVER.findAll(postRequestedFor(urlEqualTo(c.path()))).getFirst();
            assertThat(request.getHeader("Idempotency-Key")).isEqualTo("a1:" + c.kind().name());
            var body = JsonCodec.readTree(request.getBodyAsString());
            assertThat(body.get(c.recipientField()).asString()).isEqualTo(c.recipientValue());
            assertThat(body.get("alertId").asString()).isEqualTo("a1");
        }
    }

    @Test
    @DisplayName("a mensagem ao cliente é genérica: sem token da conta, sem regras, sem pontuação e sem valores")
    void mensagemGenerica() {
        for (var c : cases(Duration.ofSeconds(2))) {
            SERVER.resetAll();
            SERVER.stubFor(post(urlEqualTo(c.path())).willReturn(aResponse().withStatus(202).withBody("{}")));

            c.channel().send(outbound(c, CONTACT));

            var body = SERVER.findAll(postRequestedFor(urlEqualTo(c.path()))).getFirst().getBodyAsString();
            assertThat(body).as(c.name()).doesNotContain(TestAlerts.ACCOUNT).doesNotContain("R10").doesNotContain("lista de bloqueio")
                    .doesNotContain("score").doesNotContain("traceId");
        }
    }

    @Test
    @DisplayName("503 e timeout: nova tentativa; 400: recusa definitiva")
    void falhas() {
        for (var c : cases(Duration.ofMillis(400))) {
            SERVER.resetAll();
            SERVER.stubFor(post(urlEqualTo(c.path())).willReturn(aResponse().withStatus(503)));
            assertThat(c.channel().send(outbound(c, CONTACT)).status()).as(c.name() + " 503").isEqualTo(Status.RETRYABLE_FAILURE);

            SERVER.stubFor(post(urlEqualTo(c.path())).willReturn(aResponse().withStatus(202).withFixedDelay(2_000)));
            assertThat(c.channel().send(outbound(c, CONTACT)).status()).as(c.name() + " timeout").isEqualTo(Status.RETRYABLE_FAILURE);

            SERVER.stubFor(post(urlEqualTo(c.path())).willReturn(aResponse().withStatus(400)));
            assertThat(c.channel().send(outbound(c, CONTACT)).status()).as(c.name() + " 400").isEqualTo(Status.PERMANENT_FAILURE);
        }
    }

    @Test
    @DisplayName("sem contato, ou sem o dado do canal, é recusa definitiva e o provedor nem é chamado")
    void semContato() {
        SERVER.resetAll();
        SERVER.stubFor(post(anyUrl()).willReturn(aResponse().withStatus(202)));
        var semDado = new Contact(null, null, null);

        for (var c : cases(Duration.ofSeconds(2))) {
            assertThat(c.channel().send(outbound(c, null)).status()).as(c.name() + " sem contato").isEqualTo(Status.PERMANENT_FAILURE);
            assertThat(c.channel().send(outbound(c, semDado)).status()).as(c.name() + " sem dado").isEqualTo(Status.PERMANENT_FAILURE);
        }
        SERVER.verify(0, anyRequestedFor(anyUrl()));
    }

    @Test
    @DisplayName("cada canal se identifica com o seu tipo")
    void identifica() {
        var kinds = new java.util.ArrayList<Channel>();
        for (var c : cases(Duration.ofSeconds(1))) {
            kinds.add(c.channel().channel());
        }

        assertThat(kinds).containsExactly(Channel.PUSH, Channel.EMAIL, Channel.SMS);
    }
}
