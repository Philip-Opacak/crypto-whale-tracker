package com.whalewatcher.ingest.offchain.websocket;

import com.whalewatcher.domain.Trade;
import com.whalewatcher.service.IngestionService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class WsWorkers {

    private static final int WS_WORKERS =
            Math.max(2, Math.min(6, Runtime.getRuntime().availableProcessors() - 1));

    private final RawWsBus bus;
    private final IngestionService ingestionService;
    private final WsMessageParser parser;
    private final ExecutorService pool;

    private volatile boolean running = true;

    public WsWorkers(RawWsBus bus, IngestionService ingestionService, WsMessageParser parser) {
        this.bus = bus;
        this.ingestionService = ingestionService;
        this.parser = parser;

        this.pool = Executors.newFixedThreadPool(WS_WORKERS);
        System.out.println("WS workers started: " + WS_WORKERS);
    }

    @PostConstruct
    public void start() {
        for (int i = 0; i < WS_WORKERS; i++) {
            pool.submit(this::loop);
        }
    }

    private void loop() {
        while (running) {
            try {
                RawWsBus.RawWsMsg m = bus.take();

                List<Trade> trades = parser.parse(m.exchange(), m.raw());
                for (Trade t : trades) {
                    ingestionService.ingest(t);
                }

            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                System.err.println("WS worker error: " + e.getMessage());
            }
        }
    }

    @PreDestroy
    public void stop() {
        running = false;
        pool.shutdownNow();
    }
}