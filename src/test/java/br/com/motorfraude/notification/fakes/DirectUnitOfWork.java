package br.com.motorfraude.notification.fakes;

import br.com.motorfraude.notification.application.gateway.UnitOfWork;
import java.util.function.Supplier;

public final class DirectUnitOfWork implements UnitOfWork {

    @Override
    public <T> T inTransaction(Supplier<T> work) {
        return work.get();
    }
}
