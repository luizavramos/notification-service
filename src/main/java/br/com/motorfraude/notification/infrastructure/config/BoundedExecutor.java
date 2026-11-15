package br.com.motorfraude.notification.infrastructure.config;

import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;

public final class BoundedExecutor implements Executor {

    private final Executor delegate;
    private final Semaphore permits;

    public BoundedExecutor(Executor delegate, int maxConcurrent) {
        this.delegate = delegate;
        this.permits = new Semaphore(maxConcurrent);
    }

    @Override
    public void execute(Runnable task) {
        try {
            permits.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrompido esperando vaga para despachar", e);
        }
        try {
            delegate.execute(() -> {
                try {
                    task.run();
                } finally {
                    permits.release();
                }
            });
        } catch (RuntimeException e) {
            permits.release();
            throw e;
        }
    }
}
