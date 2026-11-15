package br.com.motorfraude.notification.infrastructure;

import static org.assertj.core.api.Assertions.assertThat;

import br.com.motorfraude.notification.infrastructure.config.BoundedExecutor;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class BoundedExecutorTest {

    @Test
    @DisplayName("nunca roda mais tarefas ao mesmo tempo que o limite, e quem submete espera em vez de a tarefa ser descartada")
    void respeitaOLimite() throws Exception {
        var executor = new BoundedExecutor(Executors.newVirtualThreadPerTaskExecutor(), 5);
        var running = new AtomicInteger();
        var peak = new AtomicInteger();
        var done = new CountDownLatch(200);

        for (int i = 0; i < 200; i++) {
            executor.execute(() -> {
                int now = running.incrementAndGet();
                peak.accumulateAndGet(now, Math::max);
                try {
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    running.decrementAndGet();
                    done.countDown();
                }
            });
        }

        assertThat(done.await(20, TimeUnit.SECONDS)).as("todas as 200 tarefas rodaram").isTrue();
        assertThat(peak.get()).isLessThanOrEqualTo(5).isGreaterThan(1);
    }

    @Test
    @DisplayName("a licença volta mesmo quando a tarefa lança exceção")
    void liberaNaFalha() throws Exception {
        var executor = new BoundedExecutor(Executors.newVirtualThreadPerTaskExecutor(), 1);
        var done = new CountDownLatch(3);

        for (int i = 0; i < 3; i++) {
            executor.execute(() -> {
                done.countDown();
                throw new IllegalStateException("explodiu");
            });
        }

        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
    }
}
