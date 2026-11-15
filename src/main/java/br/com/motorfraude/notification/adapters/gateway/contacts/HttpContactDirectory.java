package br.com.motorfraude.notification.adapters.gateway.contacts;

import br.com.motorfraude.contracts.JsonCodec;
import br.com.motorfraude.notification.application.gateway.Contact;
import br.com.motorfraude.notification.application.gateway.ContactDirectory;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Optional;

public final class HttpContactDirectory implements ContactDirectory {

    private final HttpClient http;
    private final URI base;
    private final Duration timeout;

    public HttpContactDirectory(HttpClient http, URI base, Duration timeout) {
        this.http = http;
        this.base = base;
        this.timeout = timeout;
    }

    @Override
    public Optional<Contact> find(String accountId) {
        var uri = URI.create(base.toString().replaceAll("/+$", "") + "/v1/contacts/" + accountId);
        var request = HttpRequest.newBuilder(uri).timeout(timeout).header("Accept", "application/json").GET().build();
        try {
            var response = http.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 404) {
                return Optional.empty();
            }
            if (response.statusCode() / 100 != 2) {
                throw new UnavailableException("serviço de contatos respondeu HTTP " + response.statusCode(), null);
            }
            var node = JsonCodec.readTree(response.body());
            return Optional.of(new Contact(text(node.path("pushToken")), text(node.path("email")), text(node.path("phone"))));
        } catch (IOException e) {
            throw new UnavailableException("falha ao consultar o serviço de contatos: " + e, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UnavailableException("consulta ao serviço de contatos interrompida", e);
        } catch (RuntimeException e) {
            if (e instanceof UnavailableException) {
                throw e;
            }
            throw new UnavailableException("resposta inválida do serviço de contatos: " + e.getMessage(), e);
        }
    }

    private static String text(tools.jackson.databind.JsonNode node) {
        return node.isString() ? node.asString() : null;
    }
}
