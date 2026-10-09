package io.conjoined.api;

import java.net.http.*;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/** Owns the entire exchange lifetime, including body, cancellation, and admission permit. */
final class RequestExecutor implements AutoCloseable {
    private final HttpClient http;
    private final Semaphore permits = new Semaphore(64);
    private final Set<CompletableFuture<?>> active = ConcurrentHashMap.newKeySet();
    private final ScheduledThreadPoolExecutor deadlines = new ScheduledThreadPoolExecutor(1, task -> {
        Thread thread = new Thread(task, "conjoined-deadlines"); thread.setDaemon(true); return thread;
    });
    RequestExecutor(HttpClient http) { this.http = http; deadlines.setRemoveOnCancelPolicy(true); }
    record Response(int statusCode, byte[] body) {}
    int availablePermits() { return permits.availablePermits(); }
    <T> CompletableFuture<T> send(HttpRequest request, Duration timeout, Function<Response, T> decode) {
        if (!permits.tryAcquire()) return CompletableFuture.failedFuture(new IllegalStateException("Too many in-flight requests"));
        Operation<T> result = new Operation<>();
        AtomicReference<CompletableFuture<?>> upstream = new AtomicReference<>();
        AtomicReference<LimitedBody> body = new AtomicReference<>();
        AtomicReference<ScheduledFuture<?>> alarm = new AtomicReference<>();
        result.cleanup = ignored -> {
            var timer = alarm.get(); if (timer != null) timer.cancel(false);
            var subscriber = body.get(); if (subscriber != null) subscriber.cancel();
            var transfer = upstream.get(); if (transfer != null) transfer.cancel(true);
            active.remove(result); permits.release();
        };
        active.add(result);
        try {
            alarm.set(deadlines.schedule(() -> result.completeExceptionally(new TimeoutException("ConjoinedDB operation deadline exceeded")), timeout.toNanos(), TimeUnit.NANOSECONDS));
            var transfer = http.sendAsync(request, ignored -> {
                LimitedBody subscriber = new LimitedBody(); body.set(subscriber);
                if (result.isDone()) subscriber.cancel();
                return subscriber;
            });
            upstream.set(transfer);
            transfer.whenComplete((response, error) -> {
                if (error != null) result.completeExceptionally(error);
                else if (!result.isDone()) {
                    try { result.complete(decode.apply(new Response(response.statusCode(), response.body()))); }
                    catch (RuntimeException failure) { result.completeExceptionally(failure); }
                }
            });
            if (result.isDone()) transfer.cancel(true);
        } catch (RuntimeException error) { result.completeExceptionally(error); }
        if (result.isDone() && alarm.get() != null) alarm.get().cancel(false);
        return result;
    }
    private static final class Operation<T> extends CompletableFuture<T> {
        private java.util.function.Consumer<Boolean> cleanup;
        private final java.util.concurrent.atomic.AtomicBoolean terminal = new java.util.concurrent.atomic.AtomicBoolean();
        @Override public boolean complete(T value) {
            if (!terminal.compareAndSet(false, true)) return false;
            cleanup.accept(false); return super.complete(value);
        }
        @Override public boolean completeExceptionally(Throwable error) {
            if (!terminal.compareAndSet(false, true)) return false;
            cleanup.accept(true); return super.completeExceptionally(error);
        }
        @Override public boolean cancel(boolean interrupt) {
            if (!terminal.compareAndSet(false, true)) return false;
            cleanup.accept(true); return super.cancel(interrupt);
        }
    }
    static final class LimitedBody implements HttpResponse.BodySubscriber<byte[]> {
        private final HttpResponse.BodySubscriber<byte[]> delegate = HttpResponse.BodySubscribers.ofByteArray();
        private Flow.Subscription subscription;
        private long received;
        private boolean failed;
        @Override public CompletionStage<byte[]> getBody() { return delegate.getBody(); }
        @Override public synchronized void onSubscribe(Flow.Subscription subscription) {
            if (failed) { subscription.cancel(); return; }
            this.subscription = subscription; delegate.onSubscribe(subscription);
        }
        synchronized void cancel() {
            if (failed) return;
            failed = true;
            if (subscription != null) subscription.cancel();
            delegate.onError(new CancellationException("Exchange cancelled"));
        }
        @Override public synchronized void onNext(java.util.List<java.nio.ByteBuffer> buffers) {
            if (failed) return;
            for (var buffer : buffers) received += buffer.remaining();
            if (received > 65536) {
                failed = true; subscription.cancel(); delegate.onError(new IllegalStateException("Response too large"));
            } else delegate.onNext(buffers);
        }
        @Override public synchronized void onError(Throwable error) { if (!failed) { failed = true; delegate.onError(error); } }
        @Override public synchronized void onComplete() { if (!failed) { failed = true; delegate.onComplete(); } }
    }
    @Override public void close() {
        deadlines.shutdownNow();
        active.forEach(result -> result.cancel(true));
        http.shutdownNow();
    }
}
