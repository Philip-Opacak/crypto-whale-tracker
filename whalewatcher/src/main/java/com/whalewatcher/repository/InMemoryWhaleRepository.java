package com.whalewatcher.repository;

import com.whalewatcher.domain.WhaleEvent;
import org.springframework.stereotype.Repository;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

@Repository
public class InMemoryWhaleRepository implements  WhaleEventRepository {

    private final Deque<WhaleEvent> events = new ArrayDeque<>();

    @Override
    public void save(WhaleEvent whaleEvent) {
        events.addFirst(whaleEvent);
        if (events.size() > 1000)
            events.removeLast();

    }

    @Override
    public List<WhaleEvent> findRecent(int limit) {
        return events.stream().limit(limit).toList();
    }
}
