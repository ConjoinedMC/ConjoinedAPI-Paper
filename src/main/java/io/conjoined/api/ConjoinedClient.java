package io.conjoined.api;

import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;

/** Thread-safe, asynchronous binary key/value client. Never join futures on a server thread. */
public final class ConjoinedClient implements AutoCloseable {
    private final HttpClient http;
    private final URI endpoint;
    private final String namespace;
    private final String token;
    private final Semaphore requests = new Semaphore(64);
    private record Response(int statusCode, byte[] body) {}

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
    }
    public CompletableFuture<Optional<byte[]>> getGlobal(String key) { return get("global", key); }
    public CompletableFuture<Optional<byte[]>> getPlayer(UUID player, String key) { return get(player.toString(), key); }
    public CompletableFuture<Void> putGlobal(String key, byte[] value) { return put("global", key, value); }
    public CompletableFuture<Void> putPlayer(UUID player, String key, byte[] value) { return put(player.toString(), key, value); }
    public CompletableFuture<Void> deleteGlobal(String key) { return delete("global", key); }
    public CompletableFuture<Void> deletePlayer(UUID player, String key) { return delete(player.toString(), key); }
    private CompletableFuture<Optional<byte[]>> get(String owner, String key) {
        return send(request(owner, key).GET().build()).thenApply(response -> {
            if (response.statusCode() == 404) return Optional.empty();
            check(response, 200); return Optional.of(response.body());
        });
    }
    private CompletableFuture<Void> put(String owner, String key, byte[] value) {
        if (value.length > 65536) throw new IllegalArgumentException("Value too large");
        return send(request(owner, key).PUT(HttpRequest.BodyPublishers.ofByteArray(value.clone())).build())
                .thenApply(response -> { check(response, 204); return null; });
    }
    private CompletableFuture<Void> delete(String owner, String key) {
        return send(request(owner, key).DELETE().build()).thenApply(response -> { check(response, 204); return null; });
    }
    private HttpRequest.Builder request(String owner, String key) {
        if (!key.matches("[a-zA-Z0-9_.-]{1,128}")) throw new IllegalArgumentException("Invalid key");
        return HttpRequest.newBuilder(endpoint.resolve("/v1/" + namespace + "/" + owner + "/" + key))
                .timeout(Duration.ofSeconds(10)).header("Authorization", "Bearer " + token);
    }
    private CompletableFuture<Response> send(HttpRequest request) {
        if (!requests.tryAcquire()) return CompletableFuture.failedFuture(new IllegalStateException("Too many in-flight requests"));
        try {
            return http.sendAsync(request, ignored -> new LimitedBody()).thenApply(response ->
                    new Response(response.statusCode(), response.body()))
                    .whenComplete((response, error) -> requests.release());
        } catch (RuntimeException error) { requests.release(); return CompletableFuture.failedFuture(error); }
    }
    private static void check(Response response, int expected) {
        if (response.statusCode() != expected) throw new IllegalStateException("ConjoinedDB returned HTTP " + response.statusCode());
    }
    private static final class LimitedBody implements HttpResponse.BodySubscriber<byte[]> {
        private final HttpResponse.BodySubscriber<byte[]> delegate = HttpResponse.BodySubscribers.ofByteArray();
        private java.util.concurrent.Flow.Subscription subscription;
        private long received;
        private boolean failed;
        @Override public java.util.concurrent.CompletionStage<byte[]> getBody() { return delegate.getBody(); }
        @Override public void onSubscribe(java.util.concurrent.Flow.Subscription subscription) {
            this.subscription = subscription; delegate.onSubscribe(subscription);
        }
        @Override public void onNext(java.util.List<java.nio.ByteBuffer> buffers) {
            if (failed) return;
            for (var buffer : buffers) received += buffer.remaining();
            if (received > 65536) {
                failed = true; subscription.cancel(); delegate.onError(new IllegalStateException("Response too large"));
            } else delegate.onNext(buffers);
        }
        @Override public void onError(Throwable error) { if (!failed) delegate.onError(error); }
        @Override public void onComplete() { if (!failed) delegate.onComplete(); }
    }
    @Override public void close() { http.shutdownNow(); }
}
