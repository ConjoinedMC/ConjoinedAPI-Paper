package io.conjoined.api;

import java.net.URI;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ConjoinedClientTest {
    @Test void rejectsInsecureOriginsAndInvalidCredentials() {
        String token = "a".repeat(32);
        assertThrows(IllegalArgumentException.class, () -> new ConjoinedClient(URI.create("http://localhost"), "example", token));
        assertThrows(IllegalArgumentException.class, () -> new ConjoinedClient(URI.create("https://user@localhost"), "example", token));
        assertThrows(IllegalArgumentException.class, () -> new ConjoinedClient(URI.create("https://localhost/path"), "example", token));
        assertThrows(IllegalArgumentException.class, () -> new ConjoinedClient(URI.create("https://localhost"), "../escape", token));
    }
    @Test void validatesBeforeNetworkAccess() {
        try (var client = new ConjoinedClient(URI.create("https://localhost"), "example", "a".repeat(32))) {
            assertThrows(IllegalArgumentException.class, () -> client.getGlobal("../escape"));
            assertThrows(IllegalArgumentException.class, () -> client.getGlobal("."));
            assertThrows(IllegalArgumentException.class, () -> client.getGlobal(".."));
            assertThrows(IllegalArgumentException.class, () -> client.putGlobal("key", new byte[65537]));
        }
    }
}
