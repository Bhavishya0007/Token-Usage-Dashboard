package com.tokendashboard.aggregator;

import com.tokendashboard.model.ClientStatsSnapshot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class UsageAggregatorTest {

    @Test
    void recordAccumulatesTokensAndRequestCountForAClient() {
        UsageAggregator aggregator = new UsageAggregator();
        aggregator.record("client-A", 100);
        aggregator.record("client-A", 50);

        ClientStatsSnapshot stats = aggregator.snapshot().get("client-A");
        assertNotNull(stats);
        assertEquals(150, stats.totalTokens());
        assertEquals(2, stats.requestCount());
    }

    @Test
    void tracksMultipleClientsIndependently() {
        UsageAggregator aggregator = new UsageAggregator();
        aggregator.record("client-A", 100);
        aggregator.record("client-B", 10);
        aggregator.record("client-B", 10);

        Map<String, ClientStatsSnapshot> snapshot = aggregator.snapshot();
        assertEquals(100, snapshot.get("client-A").totalTokens());
        assertEquals(1, snapshot.get("client-A").requestCount());
        assertEquals(20, snapshot.get("client-B").totalTokens());
        assertEquals(2, snapshot.get("client-B").requestCount());
    }

    @Test
    void snapshotOfUnknownClientIsAbsent() {
        UsageAggregator aggregator = new UsageAggregator();
        aggregator.record("client-A", 5);

        assertNull(aggregator.snapshot().get("does-not-exist"));
    }

    @Test
    void emptyAggregatorHasEmptySnapshot() {
        UsageAggregator aggregator = new UsageAggregator();
        assertEquals(Map.of(), aggregator.snapshot());
    }

    @Test
    @Timeout(10)
    void concurrentRecordsFromManyThreadsAreNotLost() throws InterruptedException {
        UsageAggregator aggregator = new UsageAggregator();
        int threads = 8;
        int recordsPerThread = 1000;
        CountDownLatch start = new CountDownLatch(1);
        Thread[] workers = new Thread[threads];

        for (int i = 0; i < threads; i++) {
            workers[i] = new Thread(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                for (int j = 0; j < recordsPerThread; j++) {
                    aggregator.record("shared-client", 1);
                }
            });
            workers[i].start();
        }

        start.countDown();
        for (Thread t : workers) {
            t.join();
        }

        ClientStatsSnapshot stats = aggregator.snapshot().get("shared-client");
        assertEquals(threads * recordsPerThread, stats.totalTokens());
        assertEquals(threads * recordsPerThread, stats.requestCount());
    }
}
