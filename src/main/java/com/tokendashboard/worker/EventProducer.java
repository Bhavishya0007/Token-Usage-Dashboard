package com.tokendashboard.worker;

import com.tokendashboard.model.UsageEvent;
import com.tokendashboard.queue.BoundedEventQueue;

import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Simulates a single client making API calls that consume tokens.
 */
public final class EventProducer implements Runnable {
    private final String clientId;
    private final BoundedEventQueue<UsageEvent> queue;
    private final int eventsToEmit;
    private final int maxTokensPerEvent;
    private final int maxDelayMillis;

    public EventProducer(String clientId, BoundedEventQueue<UsageEvent> queue,
                          int eventsToEmit, int maxTokensPerEvent, int maxDelayMillis) {
        this.clientId = clientId;
        this.queue = queue;
        this.eventsToEmit = eventsToEmit;
        this.maxTokensPerEvent = maxTokensPerEvent;
        this.maxDelayMillis = maxDelayMillis;
    }

    @Override
    public void run() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        try {
            for (int i = 0; i < eventsToEmit; i++) {
                int tokens = random.nextInt(1, maxTokensPerEvent + 1);
                queue.put(new UsageEvent(clientId, tokens, Instant.now()));
                Thread.sleep(random.nextInt(maxDelayMillis));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
