package br.com.motorfraude.notification.application.gateway;

import java.util.function.Supplier;

public interface UnitOfWork {

    <T> T inTransaction(Supplier<T> work);
}
