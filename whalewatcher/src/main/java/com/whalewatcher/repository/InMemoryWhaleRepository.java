package com.whalewatcher.repository;

import com.whalewatcher.domain.OffChainWhaleEvent;
import org.springframework.stereotype.Repository;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

@Repository
public class InMemoryWhaleRepository implements  WhaleEventRepository {

    private final Deque<OffChainWhaleEvent> events = new ArrayDeque<>();

    @Override
    public void save(OffChainWhaleEvent whaleEvent) {
        events.addFirst(whaleEvent);
        if (events.size() > 1000)
            events.removeLast();

    }

    @Override
    public List<OffChainWhaleEvent> findRecent(int limit) {
        return events.stream().limit(limit).toList();
    }
}
