package br.com.motorfraude.notification.application.gateway;

import br.com.motorfraude.notification.domain.Channel;
import java.time.Duration;

public interface DeliveryMetrics {

    DeliveryMetrics NONE = new DeliveryMetrics() {
    };

    default void result(Channel channel, String result) {
    }

    default void alertToDelivery(Channel channel, Duration elapsed) {
    }

    default void deadLettered(String channel) {
    }

    default void reconciled(int count) {
    }
}
