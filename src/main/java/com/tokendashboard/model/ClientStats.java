package com.tokendashboard.model;

/**
 * Mutable accumulator for a single client. Not thread-safe on its own —
 * callers (UsageAggregator) must hold a lock while mutating or reading it.
 */
public final class ClientStats {
    private long totalTokens;
    private long requestCount;

    public void record(int tokens) {
        totalTokens += tokens;
        requestCount++;
    }

    public ClientStatsSnapshot snapshot() {
        return new ClientStatsSnapshot(totalTokens, requestCount);
    }
}
