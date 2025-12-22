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
        // Deduplicate by tx hash
        if (seenTxHashes.contains(e.txHash())) {
            return;
        }

        events.addFirst(e);
        seenTxHashes.add(e.txHash());

        // Trim event buffer
        while (events.size() > MAX_EVENTS) {
            OnChainWhaleEvent removed = events.removeLast();
            // also remove its hash
            seenTxHashes.remove(removed.txHash());
        }

        // Safety trim for hashes (in case of weird patterns)
        if (seenTxHashes.size() > MAX_HASHES) {
            Iterator<OnChainWhaleEvent> it = events.descendingIterator();
            Set<String> keep = new HashSet<>();

            while (it.hasNext() && keep.size() < MAX_EVENTS) {
                keep.add(it.next().txHash());
            }

            seenTxHashes.retainAll(keep);
        }
    }

    public synchronized List<OnChainWhaleEvent> latest(int limit) {
        return events.stream().limit(limit).toList();
    }
}