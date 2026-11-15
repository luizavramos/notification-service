package br.com.motorfraude.notification.fakes;

import br.com.motorfraude.notification.application.gateway.DeliveryMetrics;
import br.com.motorfraude.notification.domain.Channel;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public final class RecordingDeliveryMetrics implements DeliveryMetrics {

    public final List<String> results = new ArrayList<>();
    public final List<String> latencies = new ArrayList<>();
    public final List<String> deadLetters = new ArrayList<>();
    public int reconciled;

    @Override
    public void result(Channel channel, String result) {
        results.add(channel + ":" + result);
    }

    @Override
    public void alertToDelivery(Channel channel, Duration elapsed) {
        latencies.add(channel + ":" + elapsed.toMillis());
    }

    @Override
    public void deadLettered(String channel) {
        deadLetters.add(channel);
    }

    @Override
    public void reconciled(int count) {
        reconciled += count;
    }
}
