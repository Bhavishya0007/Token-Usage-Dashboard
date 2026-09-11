package com.tokendashboard;

import com.tokendashboard.aggregator.UsageAggregator;
import com.tokendashboard.dashboard.DashboardPrinter;
import com.tokendashboard.model.UsageEvent;
import com.tokendashboard.queue.BoundedEventQueue;
import com.tokendashboard.worker.AggregatorWorker;
import com.tokendashboard.worker.EventProducer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public final class Main {
    private static final int QUEUE_CAPACITY = 50;
    private static final int NUM_CONSUMERS = 3;
    private static final String[] CLIENT_IDS = {"client-A", "client-B", "client-C", "client-D"};
    private static final int EVENTS_PER_CLIENT = 40;
    private static final int MAX_TOKENS_PER_EVENT = 500;
    private static final int MAX_PRODUCER_DELAY_MS = 100;
    private static final long DASHBOARD_INTERVAL_MS = 1000;

    public static void main(String[] args) throws InterruptedException {
        BoundedEventQueue<UsageEvent> queue = new BoundedEventQueue<>(QUEUE_CAPACITY);
        UsageAggregator aggregator = new UsageAggregator();
        AtomicBoolean dashboardRunning = new AtomicBoolean(true);

        Thread dashboardThread = new Thread(
                new DashboardPrinter(aggregator, DASHBOARD_INTERVAL_MS, dashboardRunning), "dashboard");
        dashboardThread.start();

        List<Thread> consumerThreads = new ArrayList<>();
        for (int i = 0; i < NUM_CONSUMERS; i++) {
            Thread t = new Thread(new AggregatorWorker(queue, aggregator), "consumer-" + i);
            consumerThreads.add(t);
            t.start();
        }

        List<Thread> producerThreads = new ArrayList<>();
        for (String clientId : CLIENT_IDS) {
            Thread t = new Thread(new EventProducer(clientId, queue, EVENTS_PER_CLIENT,
                    MAX_TOKENS_PER_EVENT, MAX_PRODUCER_DELAY_MS), "producer-" + clientId);
            producerThreads.add(t);
            t.start();
        }

        for (Thread t : producerThreads) {
            t.join();
        }

        // One poison pill per consumer thread, so each one sees exactly one and exits.
        for (int i = 0; i < NUM_CONSUMERS; i++) {
            queue.put(AggregatorWorker.POISON_PILL);
        }
        for (Thread t : consumerThreads) {
            t.join();
        }

        dashboardRunning.set(false);
        dashboardThread.interrupt();
        dashboardThread.join();

        System.out.println("Simulation complete.");
    }
}
