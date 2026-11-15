package br.com.motorfraude.notification.infrastructure.config;

import br.com.motorfraude.notification.adapters.gateway.postgres.JdbcAlertRepository;
import br.com.motorfraude.notification.adapters.gateway.postgres.JdbcDeliveryRepository;
import br.com.motorfraude.notification.application.gateway.AlertRepository;
import br.com.motorfraude.notification.application.gateway.DeliveryRepository;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.simple.JdbcClient;

@Configuration
public class PersistenceConfig {

    @Bean(initMethod = "migrate")
    Flyway flyway(DataSource dataSource) {
        return Flyway.configure().dataSource(dataSource).load();
    }

    @Bean
    AlertRepository alertRepository(JdbcClient jdbc, Flyway flyway) {
        return new JdbcAlertRepository(jdbc);
    }

    @Bean
    DeliveryRepository deliveryRepository(JdbcClient jdbc, Flyway flyway) {
        return new JdbcDeliveryRepository(jdbc);
    }
}
