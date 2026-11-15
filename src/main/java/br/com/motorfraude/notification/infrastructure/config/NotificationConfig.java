package br.com.motorfraude.notification.infrastructure.config;

import br.com.motorfraude.notification.adapters.gateway.channels.AntifraudWebhookChannel;
import br.com.motorfraude.notification.adapters.gateway.channels.EmailChannel;
import br.com.motorfraude.notification.adapters.gateway.channels.PushChannel;
import br.com.motorfraude.notification.adapters.gateway.channels.ResilienceSettings;
import br.com.motorfraude.notification.adapters.gateway.channels.ResilientChannel;
import br.com.motorfraude.notification.adapters.gateway.channels.SmsChannel;
import br.com.motorfraude.notification.adapters.gateway.contacts.HttpContactDirectory;
import br.com.motorfraude.notification.adapters.gateway.micrometer.MicrometerDeliveryMetrics;
import br.com.motorfraude.notification.adapters.gateway.postgres.TransactionalUnitOfWork;
import br.com.motorfraude.notification.application.gateway.AlertRepository;
import br.com.motorfraude.notification.application.gateway.ContactDirectory;
import br.com.motorfraude.notification.application.gateway.DeliveryMetrics;
import br.com.motorfraude.notification.application.gateway.DeliveryRepository;
import br.com.motorfraude.notification.application.gateway.NotificationChannel;
import br.com.motorfraude.notification.application.gateway.RetryScheduler;
import br.com.motorfraude.notification.application.gateway.UnitOfWork;
import br.com.motorfraude.notification.application.usecase.DeliveryDispatcher;
import br.com.motorfraude.notification.application.usecase.DispatchDeliveryUseCase;
import br.com.motorfraude.notification.application.usecase.ReconcilePendingDeliveriesUseCase;
import br.com.motorfraude.notification.application.usecase.RegisterAlertUseCase;
import br.com.motorfraude.notification.domain.Channel;
import br.com.motorfraude.notification.domain.SuppressionPolicy;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Clock;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class NotificationConfig {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    HttpClient providerHttpClient(NotificationProperties properties) {
        return HttpClient.newBuilder()
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .connectTimeout(properties.providers().antifraud().timeout())
                .build();
    }

    @Bean
    UnitOfWork unitOfWork(PlatformTransactionManager transactionManager) {
        return new TransactionalUnitOfWork(transactionManager);
    }

    @Bean
    ContactDirectory contactDirectory(HttpClient http, NotificationProperties properties) {
        var contacts = properties.providers().contacts();
        return new HttpContactDirectory(http, URI.create(contacts.url()), contacts.timeout());
    }

    @Bean
    DeliveryMetrics deliveryMetrics(MeterRegistry registry) {
        return new MicrometerDeliveryMetrics(registry);
    }

    @Bean
    Map<Channel, NotificationChannel> channels(HttpClient http, NotificationProperties properties, MeterRegistry registry) {
        var providers = properties.providers();
        var r = properties.resilience();
        var settings = new ResilienceSettings(r.maxAttempts(), r.initialBackoff(), r.backoffMultiplier(), r.jitter(), r.slidingWindowSize(),
                r.minimumCalls(), r.failureRateThreshold(), r.openStateWait(), r.halfOpenCalls(), r.maxConcurrentCalls(), r.bulkheadMaxWait());
        var channels = new EnumMap<Channel, NotificationChannel>(Channel.class);
        for (NotificationChannel channel : List.of(
                new AntifraudWebhookChannel(http, URI.create(providers.antifraud().url()), providers.antifraud().secret(), providers.antifraud().timeout()),
                new PushChannel(http, URI.create(providers.push().url()), providers.push().timeout()),
                new EmailChannel(http, URI.create(providers.email().url()), providers.email().timeout()),
                new SmsChannel(http, URI.create(providers.sms().url()), providers.sms().timeout()))) {
            var resilient = new ResilientChannel(channel, settings);
            bindCircuitState(registry, channel.channel().name(), resilient.circuitBreaker());
            channels.put(channel.channel(), resilient);
        }
        return channels;
    }

    @Bean
    RegisterAlertUseCase registerAlertUseCase(AlertRepository alerts, DeliveryRepository deliveries, UnitOfWork unitOfWork, Clock clock,
            NotificationProperties properties, DeliveryMetrics metrics) {
        return new RegisterAlertUseCase(alerts, deliveries, new SuppressionPolicy(properties.suppressionWindow()), unitOfWork, clock, metrics);
    }

    @Bean
    DeliveryDispatcher deliveryDispatcher(AlertRepository alerts, DeliveryRepository deliveries, Map<Channel, NotificationChannel> channels,
            ContactDirectory contacts, RetryScheduler scheduler, Clock clock, NotificationProperties properties,
            DeliveryMetrics metrics) {
        var delays = properties.retry().steps().stream().map(NotificationProperties.Retry.Step::delay).toList();
        return new DeliveryDispatcher(new DispatchDeliveryUseCase(alerts, deliveries, channels, contacts, scheduler, clock, delays, metrics));
    }

    @Bean
    ReconcilePendingDeliveriesUseCase reconcilePendingDeliveriesUseCase(DeliveryRepository deliveries, DeliveryDispatcher dispatcher, Clock clock,
            NotificationProperties properties, DeliveryMetrics metrics) {
        var delays = properties.retry().steps().stream().map(NotificationProperties.Retry.Step::delay).toList();
        var reconciler = properties.reconciler();
        return new ReconcilePendingDeliveriesUseCase(deliveries, dispatcher, clock, delays, reconciler.grace(), reconciler.limit(), metrics);
    }

    private static void bindCircuitState(MeterRegistry registry, String name, CircuitBreaker breaker) {
        for (var state : CircuitBreaker.State.values()) {
            Gauge.builder("resilience4j.circuitbreaker.state", breaker, b -> b.getState() == state ? 1 : 0)
                    .tag("name", name).tag("state", state.name().toLowerCase(java.util.Locale.ROOT)).register(registry);
        }
    }
}
