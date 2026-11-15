package br.com.motorfraude.notification.adapters.controller.kafka;

import java.time.Duration;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ConsumerLoopRunner implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerLoopRunner.class);
    private static final Duration POLL = Duration.ofMillis(500);

    private final String name;
    private final Consumer<?, ?> consumer;
    private final Runnable initialize;
    private final java.util.function.Consumer<Duration> poll;
    private volatile boolean running;
    private Thread thread;

    public ConsumerLoopRunner(String name, Consumer<?, ?> consumer, Runnable initialize, java.util.function.Consumer<Duration> poll) {
        this.name = name;
        this.consumer = consumer;
        this.initialize = initialize;
        this.poll = poll;
    }

    public synchronized void start() {
        if (thread != null) {
            return;
        }
        running = true;
        thread = Thread.ofPlatform().name(name).daemon().start(() -> {
            try {
                initialize.run();
                while (running) {
                    poll.accept(POLL);
                }
            } catch (WakeupException e) {
                LOG.info("{}: leitura encerrada", name);
            } catch (RuntimeException e) {
                LOG.error("{}: o laço de consumo parou por erro inesperado", name, e);
            } finally {
                running = false;
                consumer.close();
            }
        });
    }

    public boolean running() {
        return running;
    }

    @Override
    public synchronized void close() {
        if (thread == null) {
            return;
        }
        running = false;
        consumer.wakeup();
        try {
            thread.join(10_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        thread = null;
    }
}
