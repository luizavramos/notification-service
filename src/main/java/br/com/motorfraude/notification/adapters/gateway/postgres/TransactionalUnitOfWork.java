package br.com.motorfraude.notification.adapters.gateway.postgres;

import br.com.motorfraude.notification.application.gateway.UnitOfWork;
import java.util.function.Supplier;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

public class TransactionalUnitOfWork implements UnitOfWork {

    private final TransactionTemplate template;

    public TransactionalUnitOfWork(PlatformTransactionManager transactionManager) {
        this.template = new TransactionTemplate(transactionManager);
    }

    @Override
    public <T> T inTransaction(Supplier<T> work) {
        return template.execute(status -> work.get());
    }
}
