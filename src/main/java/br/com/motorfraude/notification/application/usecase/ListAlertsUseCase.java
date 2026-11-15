package br.com.motorfraude.notification.application.usecase;

import br.com.motorfraude.notification.application.gateway.AlertQuery;
import java.util.List;

public class ListAlertsUseCase {

    public static final int DEFAULT_LIMIT = 50;
    public static final int MAX_LIMIT = 200;

    private final AlertQuery query;

    public ListAlertsUseCase(AlertQuery query) {
        this.query = query;
    }

    public AlertPage list(AlertFilter filter, String cursor, Integer requestedLimit) {
        int limit = requestedLimit == null ? DEFAULT_LIMIT : requestedLimit;
        if (limit < 1 || limit > MAX_LIMIT) {
            throw new InvalidQueryException("limit deve estar entre 1 e " + MAX_LIMIT);
        }
        AlertCursor after = cursor == null ? null : AlertCursor.decode(cursor);
        List<AlertSummary> found = query.search(filter, after, limit + 1);
        if (found.size() <= limit) {
            return new AlertPage(found, null);
        }
        List<AlertSummary> items = found.subList(0, limit);
        AlertSummary last = items.get(limit - 1);
        return new AlertPage(items, new AlertCursor(last.detectedAt(), last.alertId()).encode());
    }
}
