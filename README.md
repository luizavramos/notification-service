# notification-service

Entrega os alertas do motor (`fraud-alerts.v1`): ao time antifraude por webhook assinado (todo alerta) e ao cliente por push, e-mail e SMS de reserva (só HIGH), sem duplicar e sem travar um canal por causa de outro. Spring Boot 4, virtual threads, porta 8085.

## Como entrega

- **Um alerta, um registro por canal.** O estado fica em `notification_delivery`, com chave `(alert_id, channel)`. Se o push dá certo e o e-mail falha, só o e-mail é tentado de novo; o que já foi enviado não se repete.
- **Retentativas.** Falha de um canal vai para os tópicos `alerts.retry.1m`, `alerts.retry.5m` e `alerts.retry.30m`, nessa ordem. Esgotadas as tentativas, o alerta vai para `alerts.dlq`.
- **Reserva.** Se o push falha de vez, o SMS entra no lugar.
- **Reconciliador.** Um job periódico retoma entregas que ficaram `PENDING` (por exemplo, o processo caiu no meio).
- **Alertas guardados.** Todo alerta recebido fica na tabela `alerts` (particionada por dia). O analista registra o veredito (`CONFIRMED_FRAUD` ou `FALSE_POSITIVE`) em `POST /v1/alerts/{alertId}/feedback`.
- **Webhook assinado.** O corpo vai com o cabeçalho `X-Signature` (HMAC-SHA256 com o segredo configurado).

Os endpoints, com request e response, estão em [`docs/endpoints.md`](https://github.com/luizavramos/fraud-detection-engine/blob/main/docs/endpoints.md) no repositório de visão geral. Saúde e métricas saem em `/actuator/health` e `/actuator/prometheus`.

## Configuração

| Variável | Padrão (desenvolvimento) | Para quê |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | brokers |
| `ALERTS_DB_URL` | `jdbc:postgresql://localhost:5432/alerts_db` | banco de alertas e entregas (o Flyway cria o esquema) |
| `ALERTS_DB_USER` / `ALERTS_DB_PASSWORD` | `motorfraude` / `motorfraude` | credenciais do banco |
| `ANTIFRAUD_URL` | `http://localhost:18090/antifraud-legacy/webhooks/fraud-alerts` | webhook do antifraude legado |
| `ANTIFRAUD_SECRET` | segredo de desenvolvimento | chave da assinatura; **troque fora do desenvolvimento** |
| `CONTACTS_URL` | `http://localhost:18090/contacts` | consulta do contato do cliente |
| `PUSH_URL`, `EMAIL_URL`, `SMS_URL` | `http://localhost:18090/...` | provedores de envio |

Os padrões apontam para os simuladores do compose (`infra/mocks`).

## Como rodar

Precisa de Java 21, Docker (os testes de integração usam Testcontainers) e dos artefatos `motor-fraude-parent`, `contracts` e `platform-commons` instalados (`./mvnw install` em cada um, nessa ordem).

```bash
./mvnw verify                  # unitários, ArchUnit e integração
./mvnw -DskipTests package     # gera target/notification-service-0.1.0.jar
```

Para subir com Kafka, Postgres e os simuladores, use o `docker-compose.yml` do repositório https://github.com/luizavramos/fraud-detection-engine (perfil `app`).

Camadas: `domain` ← `application` ← `adapters` ← `infrastructure` (conferido por ArchUnit).
