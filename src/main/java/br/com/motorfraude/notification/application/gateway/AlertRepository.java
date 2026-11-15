package br.com.motorfraude.notification.application.gateway;

import br.com.motorfraude.contracts.FraudAlert;
import java.util.Optional;

public interface AlertRepository {

    boolean insertIfAbsent(FraudAlert alert);

    Optional<FraudAlert> find(String alertId);
}
