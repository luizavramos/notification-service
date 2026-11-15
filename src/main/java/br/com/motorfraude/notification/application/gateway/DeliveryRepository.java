package br.com.motorfraude.notification.application.gateway;

import br.com.motorfraude.notification.domain.Channel;
import br.com.motorfraude.notification.domain.Delivery;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface DeliveryRepository {

    boolean insertIfAbsent(Delivery delivery);

    Optional<Delivery> find(String alertId, Channel channel);

    void update(Delivery delivery);

    List<Delivery> pendingOf(String alertId);

    List<Delivery> stalePending(Instant olderThan, int limit);

    Optional<Instant> latestCustomerNotification(String accountId, Instant around, Duration window, String excludingAlertId);
}
