package br.com.motorfraude.notification.adapters.gateway.channels;

import br.com.motorfraude.notification.application.gateway.Contact;
import br.com.motorfraude.notification.domain.Channel;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Map;

public final class EmailChannel extends CustomerProviderChannel {

    public EmailChannel(HttpClient http, URI endpoint, Duration timeout) {
        super(http, endpoint, timeout, Channel.EMAIL, "to", Contact::email);
    }

    @Override
    Map<String, Object> content() {
        return Map.of("subject", TITLE, "body",
                "Detectamos uma atividade suspeita na sua conta. Abra o aplicativo para conferir e, se não foi você, fale com o banco.");
    }
}
