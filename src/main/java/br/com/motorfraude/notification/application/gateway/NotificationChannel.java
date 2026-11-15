package br.com.motorfraude.notification.application.gateway;

import br.com.motorfraude.contracts.FraudAlert;
import br.com.motorfraude.notification.domain.Channel;

public interface NotificationChannel {

    Channel channel();

    Result send(Outbound outbound);

    record Outbound(FraudAlert alert, Contact contact, String idempotencyKey) {
    }

    record Result(Status status, String providerMessageId, String error) {

        public enum Status {
            SENT, RETRYABLE_FAILURE, PERMANENT_FAILURE
        }

        public static Result sent(String providerMessageId) {
            return new Result(Status.SENT, providerMessageId, null);
        }

        public static Result retryable(String error) {
            return new Result(Status.RETRYABLE_FAILURE, null, error);
        }

        public static Result permanent(String error) {
            return new Result(Status.PERMANENT_FAILURE, null, error);
        }
    }
}
