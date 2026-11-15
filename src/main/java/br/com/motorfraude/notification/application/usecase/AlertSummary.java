package br.com.motorfraude.notification.application.usecase;

import br.com.motorfraude.contracts.ContextCompleteness;
import br.com.motorfraude.contracts.Severity;
import java.time.Instant;
import java.util.List;

public record AlertSummary(String alertId, String transactionId, String accountId, Severity severity, int score, long ruleSetVersion,
        ContextCompleteness contextCompleteness, Instant detectedAt, List<String> ruleIds) {
}
