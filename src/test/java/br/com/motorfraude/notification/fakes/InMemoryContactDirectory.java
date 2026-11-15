package br.com.motorfraude.notification.fakes;

import br.com.motorfraude.notification.application.gateway.Contact;
import br.com.motorfraude.notification.application.gateway.ContactDirectory;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public final class InMemoryContactDirectory implements ContactDirectory {

    private final Map<String, Contact> contacts = new HashMap<>();
    private boolean unavailable;
    private int lookups;

    public void put(String accountId, Contact contact) {
        contacts.put(accountId, contact);
    }

    public void clear() {
        contacts.clear();
    }

    public void unavailable(boolean value) {
        this.unavailable = value;
    }

    public int lookups() {
        return lookups;
    }

    @Override
    public Optional<Contact> find(String accountId) {
        lookups++;
        if (unavailable) {
            throw new UnavailableException("serviço de contatos fora do ar", null);
        }
        return Optional.ofNullable(contacts.get(accountId));
    }
}
