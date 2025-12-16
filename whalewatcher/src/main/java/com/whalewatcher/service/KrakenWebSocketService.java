package com.whalewatcher.service;

import com.whalewatcher.domain.Exchange;
import com.whalewatcher.domain.Trade;

import com.google.gson.Gson;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class KrakenWebSocketService extends WebSocketClient {

    private static final Gson GSON = new Gson();

    //DTO representing a single trade from Kraken
    record KrakenTrade(String symbol, String side, double price, double qty, String timestamp) {}

    //Wrapper object for Kraken message
    record KrakenMsg(String channel, String type, List<KrakenTrade> data) {}

    private final IngestionService ingestionService;

    private final ExecutorService processingPool =
            Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    public KrakenWebSocketService(IngestionService ingestionService) {
        super(URI.create("wss://ws.kraken.com/v2"));
        this.ingestionService = ingestionService;
    }

    @PostConstruct
    public void start() {
        this.connect();
    }

    @PreDestroy
    public void stop() {
        try { this.close(); } catch (Exception ignored) {}
        processingPool.shutdownNow();
    }

    //Sends subscription message requesting trade updates
    @Override
    public void onOpen(ServerHandshake handshake) {
        String subscribeMessage = """
        {
          "method": "subscribe",
          "params": {
            "channel": "trade",
            "symbol": ["BTC/USD","ETH/USD","BNB/USD","SOL/USD","XRP/USD"]
          }
        }
        """;
        send(subscribeMessage);
    }

    @Override
    public void onMessage(String msg) {
        processingPool.submit(() -> {
            try {
                KrakenMsg parsed = GSON.fromJson(msg, KrakenMsg.class);

                // Ignore non-trade messages
                if (parsed == null) return;
                if (!"trade".equals(parsed.channel())) return;
                if (parsed.data() == null) return;

                //Single message may contain multiple trades
                for (KrakenTrade t : parsed.data()) {
                    Trade trade = new Trade(
                            Exchange.KRAKEN,
                            t.symbol(),
                            t.price(),
                            t.qty(),
                            t.side(),
                            Instant.parse(t.timestamp()).toEpochMilli()
                    );
                    ingestionService.ingest(trade);
                }

            } catch (Exception e) {
                System.err.println("Kraken parse error: " + e.getMessage());
            }
        });
    }

    @Override public void onClose(int code, String reason, boolean remote) {
        System.out.println("Kraken connection closed");
    }

    @Override public void onError(Exception ex) { ex.printStackTrace(); }
}