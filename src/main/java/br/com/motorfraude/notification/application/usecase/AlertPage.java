package br.com.motorfraude.notification.application.usecase;

import java.util.List;

public record AlertPage(List<AlertSummary> items, String nextCursor) {
}
