package br.com.motorfraude.notification.adapters.gateway.channels;

import br.com.motorfraude.contracts.JsonCodec;
import br.com.motorfraude.notification.application.gateway.NotificationChannel.Result;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

final class HttpCalls {

    private HttpCalls() {
    }

    static HttpRequest.Builder post(java.net.URI endpoint, Duration timeout, String idempotencyKey, String body) {
        return HttpRequest.newBuilder(endpoint)
                .timeout(timeout)
                .header("Content-Type", "application/json")
                .header("Idempotency-Key", idempotencyKey)
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
    }

    static Result send(HttpClient http, HttpRequest request, String idField, String fallbackId) {
        try {
            var response = http.send(request, HttpResponse.BodyHandlers.ofString());
            return interpret(response.statusCode(), response.body(), idField, fallbackId);
        } catch (HttpTimeoutException e) {
            return Result.retryable("timeout ao chamar " + request.uri().getHost() + ": " + e.getMessage());
        } catch (IOException e) {
            return Result.retryable("falha de rede ao chamar " + request.uri().getHost() + ": " + e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Result.retryable("chamada interrompida");
        }
    }

    static Result interpret(int status, String body, String idField, String fallbackId) {
        if (status >= 200 && status < 300) {
            return Result.sent(extract(body, idField, fallbackId));
        }
        if (status == 408 || status == 429 || status >= 500) {
            return Result.retryable("HTTP " + status);
        }
        if (status >= 400) {
            return Result.permanent("HTTP " + status);
        }
        return Result.permanent("HTTP " + status + " inesperado");
    }

    private static String extract(String body, String idField, String fallbackId) {
        if (body == null || body.isBlank()) {
            return fallbackId;
        }
        try {
            var node = JsonCodec.readTree(body);
            var value = node.path(idField);
            return value.isString() && !value.asString().isBlank() ? value.asString() : fallbackId;
        } catch (RuntimeException e) {
            return fallbackId;
        }
    }
}
