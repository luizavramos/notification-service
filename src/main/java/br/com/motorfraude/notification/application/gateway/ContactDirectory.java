package br.com.motorfraude.notification.application.gateway;

import java.util.Optional;

public interface ContactDirectory {

    Optional<Contact> find(String accountId);

    final class UnavailableException extends RuntimeException {

        public UnavailableException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
