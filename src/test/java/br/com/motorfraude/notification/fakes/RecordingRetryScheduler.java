package br.com.motorfraude.notification.fakes;

import br.com.motorfraude.notification.application.gateway.RetryScheduler;
import br.com.motorfraude.notification.domain.Delivery;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public final class RecordingRetryScheduler implements RetryScheduler {

    public record Scheduled(Delivery delivery, Duration delay) {
    }

    public record DeadLetter(Delivery delivery, String reason) {
    }

    public final List<Scheduled> retries = new ArrayList<>();
    public final List<DeadLetter> deadLetters = new ArrayList<>();

    @Override
    public void scheduleRetry(Delivery delivery, Duration delay) {
        retries.add(new Scheduled(delivery, delay));
    }

    @Override
    public void sendToDeadLetter(Delivery delivery, String reason) {
        deadLetters.add(new DeadLetter(delivery, reason));
    }
}
