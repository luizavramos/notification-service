package br.com.motorfraude.notification.adapters.gateway.channels;

import br.com.motorfraude.contracts.JsonCodec;
import br.com.motorfraude.notification.application.gateway.NotificationChannel;
import br.com.motorfraude.notification.domain.Channel;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

public final class AntifraudWebhookChannel implements NotificationChannel {

    private static final String ALGORITHM = "HmacSHA256";

    private final HttpClient http;
    private final URI endpoint;
    private final byte[] secret;
    private final Duration timeout;

    public AntifraudWebhookChannel(HttpClient http, URI endpoint, String secret, Duration timeout) {
        this.http = http;
        this.endpoint = endpoint;
        this.secret = secret.getBytes(StandardCharsets.UTF_8);
        this.timeout = timeout;
    }

    @Override
    public Channel channel() {
        return Channel.ANTIFRAUD_WEBHOOK;
    }

    @Override
    public Result send(Outbound outbound) {
        var body = JsonCodec.toJson(outbound.alert());
        var request = HttpCalls.post(endpoint, timeout, outbound.idempotencyKey(), body)
                .header("X-Signature", sign(body))
                .build();
        return HttpCalls.send(http, request, "caseId", outbound.idempotencyKey());
    }

    String sign(String body) {
        try {
            var mac = Mac.getInstance(ALGORITHM);
            mac.init(new SecretKeySpec(secret, ALGORITHM));
            return HexFormat.of().formatHex(mac.doFinal(body.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new IllegalStateException("não foi possível assinar o webhook", e);
        }
    }
}
