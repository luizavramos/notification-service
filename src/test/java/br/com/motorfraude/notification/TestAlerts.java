package br.com.motorfraude.notification;

import br.com.motorfraude.contracts.ContextCompleteness;
import br.com.motorfraude.contracts.FraudAlert;
import br.com.motorfraude.contracts.RuleHit;
import br.com.motorfraude.contracts.Severity;
import java.time.Instant;
import java.util.List;

public final class TestAlerts {

    public static final Instant NOW = Instant.parse("2026-11-15T12:00:00Z");
    public static final String ACCOUNT = "acc_tok_00000001";

    private TestAlerts() {
    }

    public static String id(int n) {
        return "00000000-0000-5000-8000-%012d".formatted(n);
    }

    public static FraudAlert alert(String alertId, String transactionId, Severity severity, Instant detectedAt) {
        return new FraudAlert(alertId, transactionId, ACCOUNT, severity, severity == Severity.HIGH ? 80 : 50,
                List.of(new RuleHit("R10", 1, 70, "lista de bloqueio")), 7, ContextCompleteness.FULL, detectedAt.minusMillis(80), detectedAt,
                "4bf92f3577b34da6a3ce929d0e0e4736");
    }

    public static FraudAlert high(String alertId) {
        return alert(alertId, "tx-" + alertId + "-abcdef", Severity.HIGH, NOW);
    }

    public static FraudAlert medium(String alertId) {
        return alert(alertId, "tx-" + alertId + "-abcdef", Severity.MEDIUM, NOW);
    }
}
