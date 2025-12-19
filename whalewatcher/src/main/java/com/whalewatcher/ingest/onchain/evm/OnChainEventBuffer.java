package com.whalewatcher.ingest.onchain.evm;

import com.whalewatcher.domain.OnChainWhaleEvent;
import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

@Component
public class OnChainEventBuffer {
    private final Deque<OnChainWhaleEvent> events = new ArrayDeque<>();

    public synchronized void add(OnChainWhaleEvent e) {
        events.addFirst(e);
        while (events.size() > 500) events.removeLast(); // keep last 500
    }

    public synchronized List<OnChainWhaleEvent> latest(int limit) {
        return events.stream().limit(limit).toList();
    }
}
