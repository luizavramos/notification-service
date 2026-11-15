package br.com.motorfraude.notification.application.usecase;

public class AlertNotFoundException extends RuntimeException {

    public AlertNotFoundException(String alertId) {
        super("alerta não encontrado: " + alertId);
    }
}
