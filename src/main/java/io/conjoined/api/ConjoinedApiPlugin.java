package io.conjoined.api;

import org.bukkit.plugin.java.JavaPlugin;

/** Lifecycle entry point. Clients are created by consuming plugins with their own credentials. */
public final class ConjoinedApiPlugin extends JavaPlugin {
    @Override public void onEnable() {
        getLogger().info("ConjoinedAPI ready. Create a ConjoinedClient with a scoped service token.");
    }
}
