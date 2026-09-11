package com.tokendashboard.worker;

import com.tokendashboard.aggregator.UsageAggregator;
import com.tokendashboard.model.UsageEvent;
import com.tokendashboard.queue.BoundedEventQueue;

/**
 * Consumer that drains events off the shared queue and folds them into the
 * aggregator. Shuts down cleanly when it dequeues the POISON_PILL sentinel,
 * one of which must be enqueued per running worker.
 */
public final class AggregatorWorker implements Runnable {
    public static final UsageEvent POISON_PILL = new UsageEvent("__POISON__", 0, null);

    private final BoundedEventQueue<UsageEvent> queue;
    private final UsageAggregator aggregator;

    public AggregatorWorker(BoundedEventQueue<UsageEvent> queue, UsageAggregator aggregator) {
        this.queue = queue;
        this.aggregator = aggregator;
    }

    @Override
    public void run() {
        try {
            while (true) {
                UsageEvent event = queue.take();
                if (event == POISON_PILL) {
                    break;
                }
                aggregator.record(event.clientId(), event.tokens());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
