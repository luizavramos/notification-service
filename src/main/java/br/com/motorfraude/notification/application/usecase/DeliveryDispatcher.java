package br.com.motorfraude.notification.application.usecase;

import br.com.motorfraude.notification.domain.Channel;
import br.com.motorfraude.notification.domain.Delivery;

public final class DeliveryDispatcher {

    private final DispatchDeliveryUseCase dispatch;

    public DeliveryDispatcher(DispatchDeliveryUseCase dispatch) {
        this.dispatch = dispatch;
    }

    public void dispatch(Delivery delivery) {
        dispatch(delivery.alertId(), delivery.channel());
    }

    public void dispatch(String alertId, Channel channel) {
        var result = dispatch.dispatch(alertId, channel);
        result.fallbacks().forEach(this::dispatch);
    }
}
