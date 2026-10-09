package io.conjoined.api;

import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;


/** Thread-safe, asynchronous binary key/value client. Never join futures on a server thread. */
public final class ConjoinedClient implements AutoCloseable {
    private final HttpClient http;
    private final URI endpoint;
    private final String namespace;
    private final String token;
    private final RequestExecutor requests;

    public ConjoinedClient(URI endpoint, String namespace, String token) {
        if (!"https".equals(endpoint.getScheme()) || endpoint.getHost() == null || endpoint.getUserInfo() != null
                || endpoint.getQuery() != null || endpoint.getFragment() != null
                || !(endpoint.getPath().isEmpty() || endpoint.getPath().equals("/")))
            throw new IllegalArgumentException("Endpoint must be an HTTPS origin");
        if (!namespace.matches("[a-z0-9_-]{1,64}") || !token.matches("[A-Za-z0-9_-]{32,256}"))
            throw new IllegalArgumentException("Invalid namespace or token");
        this.endpoint = endpoint; this.namespace = namespace; this.token = token;
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5))
                .followRedirects(HttpClient.Redirect.NEVER).build();
        this.requests = new RequestExecutor(http);
    }
    public CompletableFuture<Optional<byte[]>> getGlobal(String key) { return get("global", key); }
    public CompletableFuture<Optional<byte[]>> getPlayer(UUID player, String key) { return get(player.toString(), key); }
    public CompletableFuture<Void> putGlobal(String key, byte[] value) { return put("global", key, value); }
    public CompletableFuture<Void> putPlayer(UUID player, String key, byte[] value) { return put(player.toString(), key, value); }
    public CompletableFuture<Void> deleteGlobal(String key) { return delete("global", key); }
    public CompletableFuture<Void> deletePlayer(UUID player, String key) { return delete(player.toString(), key); }
    private CompletableFuture<Optional<byte[]>> get(String owner, String key) {
        return send(request(owner, key).GET().build(), response -> {
            if (response.statusCode() == 404) return Optional.empty();
            check(response, 200); return Optional.of(response.body());
        });
    }
    private CompletableFuture<Void> put(String owner, String key, byte[] value) {
        if (value.length > 65536) throw new IllegalArgumentException("Value too large");
        return send(request(owner, key).PUT(HttpRequest.BodyPublishers.ofByteArray(value.clone())).build(), response -> { check(response, 204); return null; });
    }
    private CompletableFuture<Void> delete(String owner, String key) {
        return send(request(owner, key).DELETE().build(), response -> { check(response, 204); return null; });
    }
    private HttpRequest.Builder request(String owner, String key) {
        if (!key.matches("[a-zA-Z0-9_.-]{1,128}") || key.equals(".") || key.equals("..")) throw new IllegalArgumentException("Invalid key");
        return HttpRequest.newBuilder(endpoint.resolve("/v1/" + namespace + "/" + owner + "/" + key))
                .timeout(Duration.ofSeconds(10)).header("Authorization", "Bearer " + token);
    }
    private <T> CompletableFuture<T> send(HttpRequest request, java.util.function.Function<RequestExecutor.Response, T> decode) {
        return requests.send(request, Duration.ofSeconds(10), decode);
    }
    private static void check(RequestExecutor.Response response, int expected) {
        if (response.statusCode() != expected) throw new IllegalStateException("ConjoinedDB returned HTTP " + response.statusCode());
    }
    @Override public void close() { requests.close(); }
}
