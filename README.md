# ConjoinedAPI for Paper

Java 21 asynchronous client and lightweight Paper plugin targeting Paper 1.21.4.
The API talks to ConjoinedDB on Velocity using HTTPS; it never opens the database directly.
Each consuming plugin constructs its own client with a namespace-scoped service credential.

## Build and install

With JDK 21 and Maven 3.9+, run `mvn verify`. Copy
`target/conjoinedapi-paper-0.1.0-SNAPSHOT.jar` into Paper's `plugins` folder.
Consuming plugins declare `depend: [ConjoinedAPI]` in their `plugin.yml` and compile against
this artifact with Maven scope `provided`. Run `mvn install` to make the snapshot available locally.
Do not bundle another copy of the API plugin into a consumer.

## Usage

```java
var client = new ConjoinedClient(
    URI.create("https://db.example.com:8443"),
    "example", System.getenv("CONJOINED_TOKEN_EXAMPLE"));
client.putPlayer(playerId, "settings", payload)
    .thenCompose(ignored -> client.getPlayer(playerId, "settings"))
    .thenAccept(value -> { /* Handle the bytes without touching Bukkit world state here. */ })
    .exceptionally(error -> { /* Report failure without logging credentials. */ return null; });
// Call client.close() when the consuming plugin is disabled.
```

Use `getGlobal`, `putGlobal`, and `deleteGlobal` for non-player data, or their Player
counterparts with a UUID. A missing value returns `Optional.empty()`.
The caller owns serialization and schema versioning. No client cache is used, avoiding
stale values across backend servers. The client limits concurrent requests to 64 and
uses a 5-second connect timeout and a 10-second deadline covering headers and body.
Deadline expiration, explicit cancellation and client shutdown cancel the upstream exchange
and recover its admission permit. Cancel the original returned future to stop an operation;
Java does not propagate cancellation from arbitrary downstream dependent futures. It never retries writes.

Do not call `join()` or `get()` on server threads. Completion callbacks run outside the
server scheduler. Schedule Bukkit entity/world work through the appropriate server scheduler.
Do not expose the namespace token or unrestricted client to untrusted plugins.
Use a certificate trusted by the JVM; configure a truststore for a private CA.

Read [the protocol and consistency limits](docs/PROTOCOL.md). Tests verify validation, stalled-body deadlines, cancellation and permit recovery;
deployment still requires a live Paper/Velocity integration check.
Licensed under [GPL-3.0-only](LICENSE).

CI verifies checksums for the full resolved Java dependency graph and scans advisories
and Git history. Provided-dependency overrides do not patch the running Paper distribution;
keep the server itself updated.
