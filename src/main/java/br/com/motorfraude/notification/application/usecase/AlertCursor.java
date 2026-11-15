package br.com.motorfraude.notification.application.usecase;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;

public record AlertCursor(Instant detectedAt, String alertId) {

    public String encode() {
        long micros = ChronoUnit.MICROS.between(Instant.EPOCH, detectedAt);
        return Base64.getUrlEncoder().withoutPadding().encodeToString((micros + "|" + alertId).getBytes(StandardCharsets.UTF_8));
    }

    public static AlertCursor decode(String text) {
        try {
            String raw = new String(Base64.getUrlDecoder().decode(text), StandardCharsets.UTF_8);
            int bar = raw.indexOf('|');
            if (bar <= 0 || bar == raw.length() - 1) {
                throw new IllegalArgumentException("formato");
            }
            return new AlertCursor(Instant.EPOCH.plus(Long.parseLong(raw.substring(0, bar)), ChronoUnit.MICROS), raw.substring(bar + 1));
        } catch (RuntimeException e) {
            throw new InvalidQueryException("cursor inválido");
        }
    }
}
