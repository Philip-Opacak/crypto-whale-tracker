package com.whalewatcher;

import com.whalewatcher.domain.Exchange;
import com.whalewatcher.ingest.offchain.websocket.RawWsBus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class RawWsBusTest {

    private RawWsBus bus;

    @BeforeEach
    void setup() {
        bus = new RawWsBus();
    }

    @Test
    void publish_thenTake_returnsSameExchangeAndRaw() throws Exception {
        Exchange ex = Exchange.BINANCE;
        String raw = "{\"some\":\"message\"}";

        bus.publish(ex, raw);
        RawWsBus.RawWsMsg msg = bus.take();

        // ensure message taken from the dequeue is the same as the message published
        assertEquals(ex, msg.exchange());
        assertEquals(raw, msg.raw());
    }

    @Test
    void take_blocksUntilMessageIsPublished() throws Exception {
        ExecutorService exec = Executors.newSingleThreadExecutor();
        try {
            AtomicReference<RawWsBus.RawWsMsg> ref = new AtomicReference<>();

            Future<?> f = exec.submit(() -> {
                try {
                    // bus blocks take() when empty and unblocks when publish() is called
                    ref.set(bus.take()); // should block until publish happens
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            Thread.sleep(50);

            // publish should unblock take()
            bus.publish(Exchange.KRAKEN, "{\"some\":message}");

            // should complete
            f.get(500, TimeUnit.MILLISECONDS);

            RawWsBus.RawWsMsg msg = ref.get();
            assertNotNull(msg);
            assertEquals(Exchange.KRAKEN, msg.exchange());

        } finally {
            exec.shutdownNow();
        }
    }

    @Test
    void publish_setsReceivedAtTimestamp() throws Exception {
        long before = System.currentTimeMillis();

        bus.publish(Exchange.COINBASE, "{\"message\":some}");
        RawWsBus.RawWsMsg msg = bus.take();

        long after = System.currentTimeMillis();

        assertTrue(msg.receivedAtMs() >= before, "receivedAtMs should be >= before");
        assertTrue(msg.receivedAtMs() <= after, "receivedAtMs should be <= after");
    }

    @Test
    void publish_whenQueueAtCapacity_doesNotExceedCapacity() {
        // Fill beyond capacity
        for (int i = 0; i < 210_000; i++) {
            bus.publish(Exchange.BINANCE, "{\"message\":" + i + "}");
        }

        // queue size must cap at capacity
        assertEquals(200_000, bus.size());
    }

    @Test
    void publish_whenQueueFull_incrementsDroppedCounter() {
        // fill beyond capacity
        for (int i = 0; i < 210_000; i++) {
            bus.publish(Exchange.BINANCE, "{\"message\":" + i + "}");
        }

        //dropped must be > 0
        assertEquals(200_000, bus.size());
        assertTrue(bus.droppedCount() > 0, "Expected droppedCount to increase when queue is full");
    }
}