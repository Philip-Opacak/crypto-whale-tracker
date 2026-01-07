package com.whalewatcher.ingest.onchain.evm;

import com.whalewatcher.domain.OnChainWhaleEvent;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class OnChainEventBuffer {

    private static final int MAX_EVENTS = 500;
    private static final int MAX_HASHES = 5000;

    private final Deque<OnChainWhaleEvent> events = new ArrayDeque<>();
    private final Set<String> seenTxHashes = new HashSet<>();

    public synchronized void add(OnChainWhaleEvent e) {
        String key = e.chain().name() + ":" + e.txHash();

        if (seenTxHashes.contains(key)) return;

        events.addFirst(e);
        seenTxHashes.add(key);

        while (events.size() > MAX_EVENTS) {
            OnChainWhaleEvent removed = events.removeLast();
            seenTxHashes.remove(removed.chain().name() + ":" + removed.txHash());
        }

        if (seenTxHashes.size() > MAX_HASHES) {
            Iterator<OnChainWhaleEvent> it = events.descendingIterator();
            Set<String> keep = new HashSet<>();

            while (it.hasNext() && keep.size() < MAX_EVENTS) {
                OnChainWhaleEvent ev = it.next();
                keep.add(ev.chain().name() + ":" + ev.txHash());
            }

            seenTxHashes.retainAll(keep);
        }
    }

    public synchronized List<OnChainWhaleEvent> latest(int limit) {
        int safeLimit = Math.max(0, Math.min(limit, MAX_EVENTS));
        return events.stream().limit(safeLimit).toList();
    }
}