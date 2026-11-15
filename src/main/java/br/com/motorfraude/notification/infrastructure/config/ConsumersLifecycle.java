package br.com.motorfraude.notification.infrastructure.config;

import br.com.motorfraude.notification.adapters.controller.kafka.ConsumerLoopRunner;
import br.com.motorfraude.notification.application.usecase.ReconcilePendingDeliveriesUseCase;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

public class ConsumersLifecycle implements SmartLifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumersLifecycle.class);

    private final List<ConsumerLoopRunner> runners;
    private final ReconcilePendingDeliveriesUseCase reconciler;
    private final Duration reconcileEvery;
    private ScheduledExecutorService scheduler;
    private volatile boolean running;

    public ConsumersLifecycle(List<ConsumerLoopRunner> runners, ReconcilePendingDeliveriesUseCase reconciler, Duration reconcileEvery) {
        this.runners = runners;
        this.reconciler = reconciler;
        this.reconcileEvery = reconcileEvery;
    }

    @Override
    public void start() {
        runners.forEach(ConsumerLoopRunner::start);
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> Thread.ofPlatform().name("delivery-reconciler").daemon().unstarted(r));
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                int dispatched = reconciler.run();
                if (dispatched > 0) {
                    LOG.info("reconciliador: {} entrega(s) PENDING despachada(s)", dispatched);
                }
            } catch (RuntimeException e) {
                LOG.warn("reconciliador: rodada falhou, tenta de novo na próxima: {}", e.toString());
            }
        }, reconcileEvery.toMillis(), reconcileEvery.toMillis(), TimeUnit.MILLISECONDS);
        running = true;
    }

    @Override
    public void stop() {
        running = false;
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        runners.forEach(ConsumerLoopRunner::close);
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    public boolean allRunning() {
        return running && runners.stream().allMatch(ConsumerLoopRunner::running);
    }
}
