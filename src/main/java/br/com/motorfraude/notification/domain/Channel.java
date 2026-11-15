package br.com.motorfraude.notification.domain;

public enum Channel {
    ANTIFRAUD_WEBHOOK(false),
    PUSH(true),
    EMAIL(true),
    SMS(true);

    private final boolean customerFacing;

    Channel(boolean customerFacing) {
        this.customerFacing = customerFacing;
    }

    public boolean customerFacing() {
        return customerFacing;
    }
}
