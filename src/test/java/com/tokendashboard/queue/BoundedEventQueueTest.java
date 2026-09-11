package com.tokendashboard.queue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BoundedEventQueueTest {

    @Test
    void rejectsNonPositiveCapacity() {
        assertThrows(IllegalArgumentException.class, () -> new BoundedEventQueue<Integer>(0));
        assertThrows(IllegalArgumentException.class, () -> new BoundedEventQueue<Integer>(-1));
    }

    @Test
    void putThenTakePreservesFifoOrder() throws InterruptedException {
        BoundedEventQueue<Integer> queue = new BoundedEventQueue<>(5);
        queue.put(1);
        queue.put(2);
        queue.put(3);

        assertEquals(1, queue.take());
        assertEquals(2, queue.take());
        assertEquals(3, queue.take());
    }

    @Test
    void sizeReflectsCurrentContents() throws InterruptedException {
        BoundedEventQueue<String> queue = new BoundedEventQueue<>(3);
        assertEquals(0, queue.size());
        queue.put("a");
        queue.put("b");
        assertEquals(2, queue.size());
        queue.take();
        assertEquals(1, queue.size());
    }

    @Test
    @Timeout(5)
    void putBlocksWhenFullUntilSpaceFreed() throws InterruptedException {
        BoundedEventQueue<Integer> queue = new BoundedEventQueue<>(1);
        queue.put(1);

        AtomicBoolean putReturned = new AtomicBoolean(false);
        CountDownLatch aboutToBlock = new CountDownLatch(1);
        Thread producer = new Thread(() -> {
            try {
                aboutToBlock.countDown();
                queue.put(2);
                putReturned.set(true);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        producer.start();

        aboutToBlock.await();
        Thread.sleep(200); // let the producer actually enter the blocking put()
        assertFalse(putReturned.get(), "put() should still be blocked while queue is full");

        queue.take(); // frees a slot, should unblock the producer
        producer.join();
        assertTrue(putReturned.get(), "put() should have returned once space freed up");
    }

    @Test
    @Timeout(5)
    void takeBlocksWhenEmptyUntilItemAvailable() throws InterruptedException {
        BoundedEventQueue<Integer> queue = new BoundedEventQueue<>(1);
        AtomicBoolean tookItem = new AtomicBoolean(false);

        Thread consumer = new Thread(() -> {
            try {
                queue.take();
                tookItem.set(true);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        consumer.start();

        Thread.sleep(200);
        assertFalse(tookItem.get(), "take() should still be blocked while queue is empty");

        queue.put(42);
        consumer.join();
        assertTrue(tookItem.get());
    }

    @Test
    @Timeout(10)
    void concurrentProducersAndConsumerDoNotLoseItems() throws InterruptedException {
        BoundedEventQueue<Integer> queue = new BoundedEventQueue<>(10);
        int producers = 4;
        int itemsPerProducer = 500;
        int totalItems = producers * itemsPerProducer;

        Thread[] producerThreads = new Thread[producers];
        for (int p = 0; p < producers; p++) {
            producerThreads[p] = new Thread(() -> {
                try {
                    for (int i = 0; i < itemsPerProducer; i++) {
                        queue.put(1);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        AtomicInteger consumed = new AtomicInteger();
        Thread consumerThread = new Thread(() -> {
            try {
                for (int i = 0; i < totalItems; i++) {
                    queue.take();
                    consumed.incrementAndGet();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        consumerThread.start();
        for (Thread t : producerThreads) {
            t.start();
        }
        for (Thread t : producerThreads) {
            t.join();
        }
        consumerThread.join();

        assertEquals(totalItems, consumed.get());
        assertEquals(0, queue.size());
    }
}
