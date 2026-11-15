-- notification-service (spec 005): alertas, entregas por canal e feedback do analista.

-- Alertas, particionados por dia de detected_at (a retenção é um DROP da partição velha).
-- No Postgres, PRIMARY KEY e UNIQUE de tabela particionada precisam incluir a chave de partição; por isso a unicidade aqui é por
-- (alert_id, detected_at) e (transaction_id, detected_at). A idempotência do EFEITO (uma entrega por canal) não depende disso: ela vem
-- da PK (alert_id, channel) de notification_delivery, que não é particionada.
CREATE TABLE alerts (
    alert_id             text        NOT NULL,
    transaction_id       text        NOT NULL,
    account_token        text        NOT NULL,
    severity             text        NOT NULL CHECK (severity IN ('MEDIUM', 'HIGH')),
    score                integer     NOT NULL CHECK (score >= 0),
    rule_set_version     bigint      NOT NULL,
    context_completeness text        NOT NULL CHECK (context_completeness IN ('FULL', 'PARTIAL')),
    event_received_at    timestamptz NOT NULL,
    detected_at          timestamptz NOT NULL,
    trace_id             text        NOT NULL,
    payload              jsonb       NOT NULL,
    created_at           timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (alert_id, detected_at),
    UNIQUE (transaction_id, detected_at)
) PARTITION BY RANGE (detected_at);

CREATE INDEX alerts_account_detected_idx ON alerts (account_token, detected_at DESC);
CREATE INDEX alerts_alert_id_idx ON alerts (alert_id);

-- Cria a partição do dia (UTC) se ainda não existir; seguro contra duas instâncias criando a mesma ao mesmo tempo.
CREATE FUNCTION ensure_alert_partition(day date) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    partition_name text := format('alerts_%s', to_char(day, 'YYYY_MM_DD'));
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('ensure_alert_partition'));
    IF to_regclass(partition_name) IS NULL THEN
        EXECUTE format('CREATE TABLE %I PARTITION OF alerts FOR VALUES FROM (%L) TO (%L)',
                       partition_name, day::text || ' 00:00:00+00', (day + 1)::text || ' 00:00:00+00');
    END IF;
END;
$$;

-- Uma entrega por alerta e por canal: a chave que garante "nunca avisar duas vezes".
CREATE TABLE notification_delivery (
    alert_id        text        NOT NULL,
    channel         text        NOT NULL CHECK (channel IN ('ANTIFRAUD_WEBHOOK', 'PUSH', 'EMAIL', 'SMS')),
    status          text        NOT NULL CHECK (status IN ('PENDING', 'SENT', 'FAILED', 'SUPPRESSED')),
    attempts        integer     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error      text,
    provider_msg_id text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL,
    PRIMARY KEY (alert_id, channel)
);

-- Para o reconciliador (entregas PENDING paradas).
CREATE INDEX notification_delivery_status_updated_idx ON notification_delivery (status, updated_at);

-- Rótulos do analista para calibrar as regras.
CREATE TABLE alert_feedback (
    id         bigserial   PRIMARY KEY,
    alert_id   text        NOT NULL,
    verdict    text        NOT NULL CHECK (verdict IN ('CONFIRMED_FRAUD', 'FALSE_POSITIVE')),
    analyst    text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX alert_feedback_alert_idx ON alert_feedback (alert_id);
