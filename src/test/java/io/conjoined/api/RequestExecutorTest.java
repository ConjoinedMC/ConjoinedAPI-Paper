package io.conjoined.api;

import com.sun.net.httpserver.HttpServer;
import java.net.*;
import java.net.http.*;
import java.time.Duration;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RequestExecutorTest {
    @Test void completionDeadlineAndCancellationRacesDoNotLeakPermits() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 64);
        try (var workers = Executors.newVirtualThreadPerTaskExecutor();
             var requests = new RequestExecutor(HttpClient.newHttpClient())) {
            server.setExecutor(workers);
            server.createContext("/", exchange -> {
                try (exchange) { exchange.sendResponseHeaders(200, 1); exchange.getResponseBody().write(1); }
                catch (java.io.IOException expected) { }
            });
            server.start();
            var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.getAddress().getPort())).build();
            var operations = new java.util.ArrayList<CompletableFuture<?>>();
            for (int index = 0; index < 64; index++) {
                var operation = requests.send(request, Duration.ofMillis(10), value -> value);
                operations.add(operation);
                workers.submit(() -> operation.cancel(true));
            }
            for (var operation : operations) {
                try { operation.get(3, TimeUnit.SECONDS); }
                catch (CancellationException | ExecutionException expected) { }
            }
            assertEquals(64, requests.availablePermits());
        } finally { server.stop(0); }
    }
    @Test void stalledBodiesExpireAndRecoverAllPermits() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 64);
        CountDownLatch release = new CountDownLatch(1);
        try (var workers = Executors.newVirtualThreadPerTaskExecutor();
             var requests = new RequestExecutor(HttpClient.newHttpClient())) {
            server.setExecutor(workers);
            server.createContext("/", exchange -> {
                try (exchange) {
                    exchange.sendResponseHeaders(200, 1); exchange.getResponseBody().flush();
                    release.await(5, TimeUnit.SECONDS); exchange.getResponseBody().write(1);
                } catch (Exception ignored) { /* Cancellation closes these connections. */ }
            });
            server.start();
            URI url = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
            var pending = new java.util.ArrayList<CompletableFuture<?>>();
            for (int i = 0; i < 64; i++) pending.add(requests.send(HttpRequest.newBuilder(url).build(), Duration.ofMillis(500), value -> value));
            for (var operation : pending) assertThrows(ExecutionException.class, () -> operation.get(3, TimeUnit.SECONDS));
            assertEquals(64, requests.availablePermits());
            release.countDown();
            var recovered = requests.send(HttpRequest.newBuilder(url).build(), Duration.ofSeconds(3), value -> value);
            assertArrayEquals(new byte[]{1}, recovered.get(4, TimeUnit.SECONDS).body());
            assertEquals(64, requests.availablePermits());
        } finally { release.countDown(); server.stop(0); }
    }
    @Test void callerCancellationAndOversizedBodyRecoverPermits() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 8);
        CountDownLatch headers = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (var workers = Executors.newVirtualThreadPerTaskExecutor();
             var requests = new RequestExecutor(HttpClient.newHttpClient())) {
            server.setExecutor(workers);
            server.createContext("/stall", exchange -> {
                try (exchange) {
                    exchange.sendResponseHeaders(200, 1); exchange.getResponseBody().flush();
                    headers.countDown(); release.await(5, TimeUnit.SECONDS);
                } catch (Exception ignored) { }
            });
            server.createContext("/large", exchange -> {
                try (exchange) { exchange.sendResponseHeaders(200, 65537); exchange.getResponseBody().write(new byte[65537]); }
            });
            server.start();
            String origin = "http://127.0.0.1:" + server.getAddress().getPort();
            var pending = requests.send(HttpRequest.newBuilder(URI.create(origin + "/stall")).build(), Duration.ofSeconds(2), value -> value);
            assertTrue(headers.await(3, TimeUnit.SECONDS)); assertTrue(pending.cancel(true));
            assertEquals(64, requests.availablePermits());
            var large = requests.send(HttpRequest.newBuilder(URI.create(origin + "/large")).build(), Duration.ofSeconds(2), value -> value);
            assertThrows(ExecutionException.class, () -> large.get(3, TimeUnit.SECONDS));
            assertEquals(64, requests.availablePermits());
        } finally { release.countDown(); server.stop(0); }
    }
}
