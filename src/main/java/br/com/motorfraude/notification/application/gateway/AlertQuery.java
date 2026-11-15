package br.com.motorfraude.notification.application.gateway;

import br.com.motorfraude.notification.application.usecase.AlertCursor;
import br.com.motorfraude.notification.application.usecase.AlertFilter;
import br.com.motorfraude.notification.application.usecase.AlertSummary;
import java.util.List;

public interface AlertQuery {

    List<AlertSummary> search(AlertFilter filter, AlertCursor after, int limit);
}
