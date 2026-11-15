package br.com.motorfraude.notification.adapters.gateway.channels;

import br.com.motorfraude.notification.application.gateway.Contact;
import br.com.motorfraude.notification.domain.Channel;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Map;

public final class SmsChannel extends CustomerProviderChannel {

    public SmsChannel(HttpClient http, URI endpoint, Duration timeout) {
        super(http, endpoint, timeout, Channel.SMS, "to", Contact::phone);
    }

    @Override
    Map<String, Object> content() {
        return Map.of("body", "Atividade suspeita na sua conta. Abra o aplicativo ou fale com o banco.");
    }
}
