package br.com.motorfraude.notification.application.gateway;

import br.com.motorfraude.notification.domain.Delivery;
import java.time.Duration;

public interface RetryScheduler {

    void scheduleRetry(Delivery delivery, Duration delay);

    void sendToDeadLetter(Delivery delivery, String reason);
}
