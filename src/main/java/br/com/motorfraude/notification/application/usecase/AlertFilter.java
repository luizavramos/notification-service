package br.com.motorfraude.notification.application.usecase;

import br.com.motorfraude.contracts.Severity;
import java.time.Instant;

public record AlertFilter(String accountId, Severity severity, Instant from) {

    public static AlertFilter none() {
        return new AlertFilter(null, null, null);
    }
}
