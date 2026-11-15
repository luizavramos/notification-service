package br.com.motorfraude.notification.fakes;

import br.com.motorfraude.notification.application.gateway.NotificationChannel;
import br.com.motorfraude.notification.domain.Channel;
import java.util.ArrayList;
import java.util.List;

public final class ScriptedChannel implements NotificationChannel {

    private final Channel channel;
    private final List<Result> script = new ArrayList<>();
    private final List<Outbound> calls = new ArrayList<>();

    public ScriptedChannel(Channel channel) {
        this.channel = channel;
    }

    public ScriptedChannel then(Result result) {
        script.add(result);
        return this;
    }

    public ScriptedChannel alwaysFails(String error) {
        for (int i = 0; i < 50; i++) {
            script.add(Result.retryable(error));
        }
        return this;
    }

    @Override
    public Channel channel() {
        return channel;
    }

    @Override
    public Result send(Outbound outbound) {
        calls.add(outbound);
        return script.isEmpty() ? Result.sent("msg-" + calls.size()) : script.removeFirst();
    }

    public List<Outbound> calls() {
        return calls;
    }
}
