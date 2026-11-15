package br.com.motorfraude.notification.adapters.gateway.channels;

import br.com.motorfraude.notification.application.gateway.NotificationChannel;
import br.com.motorfraude.notification.domain.Channel;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.bulkhead.BulkheadFullException;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.util.function.Supplier;

public final class ResilientChannel implements NotificationChannel {

    private static final class TransientFailure extends RuntimeException {

        private final transient Result result;

        TransientFailure(Result result) {
            super(result.error(), null, false, false);
            this.result = result;
        }
    }

    private final NotificationChannel delegate;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final Bulkhead bulkhead;

    public ResilientChannel(NotificationChannel delegate, ResilienceSettings settings) {
        this.delegate = delegate;
        var name = delegate.channel().name();
        this.circuitBreaker = CircuitBreaker.of(name, CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(settings.slidingWindowSize())
                .minimumNumberOfCalls(settings.minimumCalls())
                .failureRateThreshold(settings.failureRateThreshold())
                .waitDurationInOpenState(settings.openStateWait())
                .permittedNumberOfCallsInHalfOpenState(settings.halfOpenCalls())
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .recordExceptions(TransientFailure.class)
                .build());
        this.retry = Retry.of(name, RetryConfig.custom()
                .maxAttempts(settings.maxAttempts())
                .intervalFunction(IntervalFunction.ofExponentialRandomBackoff(settings.initialBackoff(), settings.backoffMultiplier(),
                        settings.jitter()))
                .retryExceptions(TransientFailure.class)
                .ignoreExceptions(CallNotPermittedException.class, BulkheadFullException.class)
                .build());
        this.bulkhead = Bulkhead.of(name, BulkheadConfig.custom()
                .maxConcurrentCalls(settings.maxConcurrentCalls())
                .maxWaitDuration(settings.bulkheadMaxWait())
                .build());
    }

    @Override
    public Channel channel() {
        return delegate.channel();
    }

    @Override
    public Result send(Outbound outbound) {
        Supplier<Result> call = () -> {
            var result = delegate.send(outbound);
            if (result.status() == Result.Status.RETRYABLE_FAILURE) {
                throw new TransientFailure(result);
            }
            return result;
        };
        var decorated = Retry.decorateSupplier(retry, Bulkhead.decorateSupplier(bulkhead, CircuitBreaker.decorateSupplier(circuitBreaker, call)));
        try {
            return decorated.get();
        } catch (TransientFailure e) {
            return e.result;
        } catch (CallNotPermittedException e) {
            return Result.retryable("circuit breaker aberto para " + delegate.channel());
        } catch (BulkheadFullException e) {
            return Result.retryable("canal " + delegate.channel() + " saturado (bulkhead cheio)");
        }
    }

    public CircuitBreaker.State circuitState() {
        return circuitBreaker.getState();
    }

    public CircuitBreaker circuitBreaker() {
        return circuitBreaker;
    }

    public Retry retry() {
        return retry;
    }

    public Bulkhead bulkhead() {
        return bulkhead;
    }
}
