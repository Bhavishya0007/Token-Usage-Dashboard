package com.tokendashboard;

import com.tokendashboard.aggregator.UsageAggregator;
import com.tokendashboard.model.ClientStatsSnapshot;
import com.tokendashboard.model.UsageEvent;
import com.tokendashboard.queue.BoundedEventQueue;
import com.tokendashboard.worker.AggregatorWorker;
import com.tokendashboard.worker.EventProducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Wires producers, a bounded queue, several consumers, and the aggregator
 * together the same way Main does, to check the whole pipeline delivers
 * every event exactly once under real concurrency.
 */
class PipelineIntegrationTest {

    @Test
    @Timeout(15)
    void allProducedEventsAreAggregatedExactlyOnce() throws InterruptedException {
        BoundedEventQueue<UsageEvent> queue = new BoundedEventQueue<>(20);
        UsageAggregator aggregator = new UsageAggregator();

        String[] clients = {"client-A", "client-B", "client-C"};
        int eventsPerClient = 200;
        int consumerCount = 4;

        List<Thread> consumers = new ArrayList<>();
        for (int i = 0; i < consumerCount; i++) {
            Thread t = new Thread(new AggregatorWorker(queue, aggregator));
            consumers.add(t);
            t.start();
        }

        List<Thread> producers = new ArrayList<>();
        for (String client : clients) {
            Thread t = new Thread(new EventProducer(client, queue, eventsPerClient, 10, 1));
            producers.add(t);
            t.start();
        }
        for (Thread t : producers) {
            t.join();
        }

        for (int i = 0; i < consumerCount; i++) {
            queue.put(AggregatorWorker.POISON_PILL);
        }
        for (Thread t : consumers) {
            t.join();
        }

        Map<String, ClientStatsSnapshot> snapshot = aggregator.snapshot();
        assertEquals(clients.length, snapshot.size());
        for (String client : clients) {
            assertEquals(eventsPerClient, snapshot.get(client).requestCount());
        }
    }
}
