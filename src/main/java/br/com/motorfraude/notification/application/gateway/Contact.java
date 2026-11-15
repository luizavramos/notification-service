package br.com.motorfraude.notification.application.gateway;

public record Contact(String pushToken, String email, String phone) {
}
