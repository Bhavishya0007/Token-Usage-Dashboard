package com.tokendashboard.aggregator;

import com.tokendashboard.model.ClientStats;
import com.tokendashboard.model.ClientStatsSnapshot;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * In-memory usage totals keyed by client, guarded by a ReadWriteLock so
 * multiple dashboard readers never block each other, only writers.
 */
public final class UsageAggregator {
    private final Map<String, ClientStats> statsByClient = new HashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public void record(String clientId, int tokens) {
        lock.writeLock().lock();
        try {
            statsByClient.computeIfAbsent(clientId, id -> new ClientStats()).record(tokens);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Map<String, ClientStatsSnapshot> snapshot() {
        lock.readLock().lock();
        try {
            Map<String, ClientStatsSnapshot> result = new TreeMap<>();
            for (Map.Entry<String, ClientStats> entry : statsByClient.entrySet()) {
                result.put(entry.getKey(), entry.getValue().snapshot());
            }
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }
}
