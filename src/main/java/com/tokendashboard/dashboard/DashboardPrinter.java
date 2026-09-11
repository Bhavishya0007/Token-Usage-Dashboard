package com.tokendashboard.dashboard;

import com.tokendashboard.aggregator.UsageAggregator;
import com.tokendashboard.model.ClientStatsSnapshot;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Periodically snapshots and prints aggregate usage while the simulation runs,
 * plus one final snapshot right before it stops.
 */
public final class DashboardPrinter implements Runnable {
    private final UsageAggregator aggregator;
    private final long intervalMillis;
    private final AtomicBoolean running;

    public DashboardPrinter(UsageAggregator aggregator, long intervalMillis, AtomicBoolean running) {
        this.aggregator = aggregator;
        this.intervalMillis = intervalMillis;
        this.running = running;
    }

    @Override
    public void run() {
        while (running.get()) {
            printSnapshot();
            try {
                Thread.sleep(intervalMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        printSnapshot();
    }

    private void printSnapshot() {
        Map<String, ClientStatsSnapshot> snapshot = aggregator.snapshot();
        System.out.println("---- Token Usage Dashboard ----");
        long grandTotal = 0;
        for (Map.Entry<String, ClientStatsSnapshot> entry : snapshot.entrySet()) {
            ClientStatsSnapshot stats = entry.getValue();
            System.out.printf("  %-10s requests=%-5d tokens=%d%n",
                    entry.getKey(), stats.requestCount(), stats.totalTokens());
            grandTotal += stats.totalTokens();
        }
        System.out.printf("  TOTAL tokens across all clients: %d%n%n", grandTotal);
    }
}
