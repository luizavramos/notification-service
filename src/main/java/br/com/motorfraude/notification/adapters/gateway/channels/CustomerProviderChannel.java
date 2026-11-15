package br.com.motorfraude.notification.adapters.gateway.channels;

import br.com.motorfraude.contracts.JsonCodec;
import br.com.motorfraude.notification.application.gateway.Contact;
import br.com.motorfraude.notification.application.gateway.NotificationChannel;
import br.com.motorfraude.notification.domain.Channel;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

abstract class CustomerProviderChannel implements NotificationChannel {

    static final String TITLE = "Atividade suspeita na sua conta";

    private final HttpClient http;
    private final URI endpoint;
    private final Duration timeout;
    private final Channel channel;
    private final String recipientField;
    private final Function<Contact, String> recipient;

    CustomerProviderChannel(HttpClient http, URI endpoint, Duration timeout, Channel channel, String recipientField,
            Function<Contact, String> recipient) {
        this.http = http;
        this.endpoint = endpoint;
        this.timeout = timeout;
        this.channel = channel;
        this.recipientField = recipientField;
        this.recipient = recipient;
    }

    @Override
    public Channel channel() {
        return channel;
    }

    @Override
    public Result send(Outbound outbound) {
        if (outbound.contact() == null) {
            return Result.permanent("contato ausente para o canal " + channel);
        }
        var to = recipient.apply(outbound.contact());
        if (to == null || to.isBlank()) {
            return Result.permanent("contato sem o dado do canal " + channel);
        }
        var payload = new LinkedHashMap<String, Object>();
        payload.put(recipientField, to);
        payload.putAll(content());
        payload.put("alertId", outbound.alert().alertId());
        var request = HttpCalls.post(endpoint, timeout, outbound.idempotencyKey(), JsonCodec.toJson(payload)).build();
        return HttpCalls.send(http, request, "messageId", outbound.idempotencyKey());
    }

    abstract Map<String, Object> content();
}
