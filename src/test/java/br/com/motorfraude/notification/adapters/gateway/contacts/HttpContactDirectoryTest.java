package br.com.motorfraude.notification.adapters.gateway.contacts;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import br.com.motorfraude.notification.application.gateway.Contact;
import br.com.motorfraude.notification.application.gateway.ContactDirectory;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class HttpContactDirectoryTest {

    @RegisterExtension
    static final WireMockExtension SERVER = WireMockExtension.newInstance().options(wireMockConfig().dynamicPort()).build();

    private static final String PATH = "/contacts/v1/contacts/acc_tok_00000001";

    private HttpContactDirectory directory(Duration timeout) {
        return new HttpContactDirectory(HttpClient.newHttpClient(), URI.create(SERVER.baseUrl() + "/contacts"), timeout);
    }

    @BeforeEach
    void reset() {
        SERVER.resetAll();
    }

    @Test
    @DisplayName("H2.3: busca o contato do cliente no serviço de contatos")
    void achaOContato() {
        SERVER.stubFor(get(urlEqualTo(PATH)).willReturn(aResponse().withStatus(200).withHeader("Content-Type", "application/json")
                .withBody("{\"accountToken\":\"acc_tok_00000001\",\"pushToken\":\"push-1\",\"email\":\"a@exemplo.invalid\",\"phone\":\"+5511900000000\"}")));

        var contact = directory(Duration.ofSeconds(2)).find("acc_tok_00000001");

        assertThat(contact).contains(new Contact("push-1", "a@exemplo.invalid", "+5511900000000"));
    }

    @Test
    @DisplayName("campos ausentes viram nulos, e campos desconhecidos são ignorados")
    void camposAusentes() {
        SERVER.stubFor(get(urlEqualTo(PATH)).willReturn(aResponse().withStatus(200).withBody("{\"email\":\"a@exemplo.invalid\",\"novo\":1}")));

        var contact = directory(Duration.ofSeconds(2)).find("acc_tok_00000001").orElseThrow();

        assertThat(contact.email()).isEqualTo("a@exemplo.invalid");
        assertThat(contact.pushToken()).isNull();
        assertThat(contact.phone()).isNull();
    }

    @Test
    @DisplayName("404 é conta sem contato: vazio, e não é falha do serviço")
    void semContato() {
        SERVER.stubFor(get(urlEqualTo(PATH)).willReturn(aResponse().withStatus(404)));

        assertThat(directory(Duration.ofSeconds(2)).find("acc_tok_00000001")).isEmpty();
    }

    @Test
    @DisplayName("500, resposta quebrada, timeout e conexão recusada são o serviço fora do ar (para a entrega tentar de novo)")
    void foraDoAr() {
        var directory = directory(Duration.ofMillis(400));

        SERVER.stubFor(get(urlEqualTo(PATH)).willReturn(aResponse().withStatus(500)));
        assertThatThrownBy(() -> directory.find("acc_tok_00000001")).isInstanceOf(ContactDirectory.UnavailableException.class);

        SERVER.stubFor(get(urlEqualTo(PATH)).willReturn(aResponse().withStatus(200).withBody("{não é json")));
        assertThatThrownBy(() -> directory.find("acc_tok_00000001")).isInstanceOf(ContactDirectory.UnavailableException.class);

        SERVER.stubFor(get(urlEqualTo(PATH)).willReturn(aResponse().withStatus(200).withBody("{}").withFixedDelay(2_000)));
        assertThatThrownBy(() -> directory.find("acc_tok_00000001")).isInstanceOf(ContactDirectory.UnavailableException.class);

        var refused = new HttpContactDirectory(HttpClient.newHttpClient(), URI.create("http://localhost:1/contacts"), Duration.ofSeconds(1));
        assertThatThrownBy(() -> refused.find("acc_tok_00000001")).isInstanceOf(ContactDirectory.UnavailableException.class);
    }

    @Test
    @DisplayName("FR-008: nada é guardado em cache: cada consulta vai ao serviço de contatos")
    void semCache() {
        SERVER.stubFor(get(urlEqualTo(PATH)).willReturn(aResponse().withStatus(200).withBody("{\"pushToken\":\"p\"}")));
        var directory = directory(Duration.ofSeconds(2));

        directory.find("acc_tok_00000001");
        directory.find("acc_tok_00000001");

        SERVER.verify(2, getRequestedFor(urlEqualTo(PATH)));
    }
}
