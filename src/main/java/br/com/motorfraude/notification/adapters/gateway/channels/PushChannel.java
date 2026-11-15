package br.com.motorfraude.notification.adapters.gateway.channels;

import br.com.motorfraude.notification.application.gateway.Contact;
import br.com.motorfraude.notification.domain.Channel;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Map;

public final class PushChannel extends CustomerProviderChannel {

    public PushChannel(HttpClient http, URI endpoint, Duration timeout) {
        super(http, endpoint, timeout, Channel.PUSH, "pushToken", Contact::pushToken);
    }

    @Override
    Map<String, Object> content() {
        return Map.of("title", TITLE, "body", "Detectamos uma atividade suspeita. Abra o aplicativo para conferir.");
    }
}
