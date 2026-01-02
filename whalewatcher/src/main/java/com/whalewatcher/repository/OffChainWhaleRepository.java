package com.whalewatcher.repository;

import com.whalewatcher.domain.Asset;
import com.whalewatcher.domain.Exchange;
import com.whalewatcher.domain.OffChainWhaleEvent;
import org.springframework.stereotype.Repository;

import java.util.Deque;
import java.util.List;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

@Repository
public class OffChainWhaleRepository implements WhaleEventRepository {

    private final Deque<OffChainWhaleEvent> events = new ConcurrentLinkedDeque<>();
    private final AtomicInteger approxSize = new AtomicInteger(0);

    private static final int MAX_SIZE = 1000;

    @Override
    public void save(OffChainWhaleEvent whaleEvent) {
        events.addFirst(whaleEvent);
        int sizeNow = approxSize.incrementAndGet();

        while (sizeNow > MAX_SIZE) {
            OffChainWhaleEvent removed = events.pollLast();
            if (removed == null) break;
            sizeNow = approxSize.decrementAndGet();
        }
    }

    @Override
    public List<OffChainWhaleEvent> getAll() {
        return getAll(Integer.MAX_VALUE);
    }

    @Override
    public List<OffChainWhaleEvent> getAllByAsset(Asset asset) {
        return getAllByAsset(asset, Integer.MAX_VALUE);
    }

    @Override
    public List<OffChainWhaleEvent> getAllByExchange(Exchange exchange) {
        return getAllByExchange(exchange, Integer.MAX_VALUE);
    }

    @Override
    public List<OffChainWhaleEvent> getByAssetAndExchange(Asset asset, Exchange exchange) {
        return getByAssetAndExchange(asset, exchange, Integer.MAX_VALUE);
    }

    @Override
    public List<OffChainWhaleEvent> getAll(int limit) {
        return takeFirstN(events, clampLimit(limit));
    }

    @Override
    public List<OffChainWhaleEvent> getAllByAsset(Asset asset, int limit) {
        int n = clampLimit(limit);
        List<OffChainWhaleEvent> out = new ArrayList<>(Math.min(n, 128));
        for (OffChainWhaleEvent e : events) {
            if (e.asset() == asset) {
                out.add(e);
                if (out.size() >= n) break;
            }
        }
        return List.copyOf(out);
    }

    @Override
    public List<OffChainWhaleEvent> getAllByExchange(Exchange exchange, int limit) {
        int n = clampLimit(limit);
        List<OffChainWhaleEvent> out = new ArrayList<>(Math.min(n, 128));
        for (OffChainWhaleEvent e : events) {
            if (e.exchange() == exchange) {
                out.add(e);
                if (out.size() >= n) break;
            }
        }
        return List.copyOf(out);
    }

    @Override
    public List<OffChainWhaleEvent> getByAssetAndExchange(Asset asset, Exchange exchange, int limit) {
        int n = clampLimit(limit);
        List<OffChainWhaleEvent> out = new ArrayList<>(Math.min(n, 128));
        for (OffChainWhaleEvent e : events) {
            if (e.asset() == asset && e.exchange() == exchange) {
                out.add(e);
                if (out.size() >= n) break;
            }
        }
        return List.copyOf(out);
    }

    // limit number of events requested to repo
    private static int clampLimit(int limit) {
        if (limit <= 0) return 1;
        return Math.min(limit, MAX_SIZE);
    }

    // create a list of the number of events needed
    private static List<OffChainWhaleEvent> takeFirstN(Iterable<OffChainWhaleEvent> it, int n) {
        List<OffChainWhaleEvent> out = new ArrayList<>(Math.min(n, 128));
        for (OffChainWhaleEvent e : it) {
            out.add(e);
            if (out.size() >= n) break;
        }
        return List.copyOf(out);
    }
}