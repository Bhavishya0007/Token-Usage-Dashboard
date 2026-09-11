package com.tokendashboard.worker;

import com.tokendashboard.aggregator.UsageAggregator;
import com.tokendashboard.model.ClientStatsSnapshot;
import com.tokendashboard.model.UsageEvent;
import com.tokendashboard.queue.BoundedEventQueue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AggregatorWorkerTest {

    @Test
    @Timeout(5)
    void consumesEventsAndRecordsThemUntilPoisonPill() throws InterruptedException {
        BoundedEventQueue<UsageEvent> queue = new BoundedEventQueue<>(10);
        UsageAggregator aggregator = new UsageAggregator();

        Thread workerThread = new Thread(new AggregatorWorker(queue, aggregator));
        workerThread.start();

        queue.put(new UsageEvent("client-A", 10, Instant.now()));
        queue.put(new UsageEvent("client-A", 20, Instant.now()));
        queue.put(new UsageEvent("client-B", 5, Instant.now()));
        queue.put(AggregatorWorker.POISON_PILL);

        workerThread.join();

        ClientStatsSnapshot a = aggregator.snapshot().get("client-A");
        ClientStatsSnapshot b = aggregator.snapshot().get("client-B");
        assertEquals(30, a.totalTokens());
        assertEquals(2, a.requestCount());
        assertEquals(5, b.totalTokens());
        assertEquals(1, b.requestCount());
    }

    @Test
    @Timeout(5)
    void stopsAfterPoisonPillWithoutProcessingFurtherEvents() throws InterruptedException {
        BoundedEventQueue<UsageEvent> queue = new BoundedEventQueue<>(10);
        UsageAggregator aggregator = new UsageAggregator();

        Thread workerThread = new Thread(new AggregatorWorker(queue, aggregator));
        workerThread.start();

        queue.put(AggregatorWorker.POISON_PILL);
        workerThread.join();

        assertFalse(workerThread.isAlive());
        assertTrue(aggregator.snapshot().isEmpty());
    }
}
