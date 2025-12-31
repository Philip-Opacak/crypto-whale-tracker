package com.whalewatcher.ingest.offchain.websocket;

import com.whalewatcher.domain.Exchange;
import org.springframework.stereotype.Component;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class RawWsBus {

    public record RawWsMsg(Exchange exchange, String raw, long receivedAtMs) {}

    private static final int WS_QUEUE_CAPACITY = 200_000;

    private final BlockingQueue<RawWsMsg> q =
            new ArrayBlockingQueue<>(WS_QUEUE_CAPACITY);

    private final AtomicLong dropped = new AtomicLong();

    public void publish(Exchange exchange, String raw) {
        boolean ok = q.offer(new RawWsMsg(exchange, raw, System.currentTimeMillis()));
        if (!ok) {
            long d = dropped.incrementAndGet();
            if (d % 10_000 == 0) {
                System.err.println("RawWsBus FULL — dropped=" + d);
            }
        }
    }

    public RawWsMsg take() throws InterruptedException {
        return q.take();
    }

    public int size() { return q.size(); }
}