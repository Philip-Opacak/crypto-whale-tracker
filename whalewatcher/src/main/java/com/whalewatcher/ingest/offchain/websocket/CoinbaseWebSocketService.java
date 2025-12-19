package com.whalewatcher.ingest.offchain.websocket;

import com.google.gson.Gson;
import com.whalewatcher.domain.Exchange;
import com.whalewatcher.domain.Trade;
import com.whalewatcher.service.IngestionService;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class CoinbaseWebSocketService extends WebSocketClient implements ExchangeStreamer {
    private static final Gson GSON = new Gson();

    //DTO representing a single trade from Coinbase
    record CoinbaseTrade(
            String trade_id,
            String product_id,
            String price,
            String size,
            String side,
            String time
    ) {}

    record CoinbaseEvent(
            String type,                 // "update"
            List<CoinbaseTrade> trades
    ) {}

    //Wrapper object for Coinbase message
    record CoinbaseMsg(
            String channel,              // "market_trades" or "heartbeats"
            String timestamp,
            List<CoinbaseEvent> events
    ) {}

    private final IngestionService ingestionService;

    private final ExecutorService processingPool =
            Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    public CoinbaseWebSocketService(IngestionService ingestionService) {
        super(URI.create("wss://advanced-trade-ws.coinbase.com"));
        this.ingestionService = ingestionService;
    }

    @Override
    public Exchange exchange() {
        return Exchange.COINBASE;
    }

    @Override
    public void start() {
        this.connect();
    }

    @Override
    public void stop() {
        try { this.close(); } catch (Exception ignored) {}
        processingPool.shutdownNow();
    }

    //Sends subscription message requesting trade updates
    @Override
    public void onOpen(ServerHandshake handshake) {
        String subscribeMessage = """
        {
            "type": "subscribe",
            "product_ids": ["BTC-USD","ETH-USD","BNB-USD","SOL-USD","XRP-USD"],
            "channel": "market_trades"
        }
        """;
        send(subscribeMessage);

        //Real-time server pings to keep all connections open
        String secondSubscribeMessage = """
        {
            "type": "subscribe",
            "channel": "heartbeats"
        }
        """;
        send(secondSubscribeMessage);
    }

    @Override
    public void onMessage(String msg) {
        processingPool.submit(() -> {
            try {
                CoinbaseMsg parsed = GSON.fromJson(msg, CoinbaseMsg.class);

                if (parsed == null) return;

                // Ignore heartbeats, subscription acks, etc.
                if (!"market_trades".equals(parsed.channel())) return;
                if (parsed.events() == null) return;
                if (parsed.events().isEmpty()) return;

                for (CoinbaseEvent evt : parsed.events()) {
                    if (evt == null || evt.trades() == null) continue;

                    for (CoinbaseTrade t : evt.trades()) {
                        if (t == null) continue;
                        if (t.price() == null || t.size() == null || t.time() == null || t.product_id() == null) continue;
                        if (t.price().isBlank() || t.size().isBlank() || t.time().isBlank() || t.product_id().isBlank()) continue;

                        Trade trade = new Trade(
                                exchange(),
                                t.product_id(),
                                Double.parseDouble(t.price()),
                                Double.parseDouble(t.size()),
                                t.side() == null
                                        ? null
                                        : t.side().toLowerCase(Locale.ROOT),
                                Instant.parse(t.time()).toEpochMilli()
                        );

                        ingestionService.ingest(trade);
                    }
                }

            } catch (Exception e) {
                System.err.println("Coinbase parse error: " + e.getMessage());
            }
        });
    }

    @Override public void onClose(int code, String reason, boolean remote) {
        System.out.println("Coinbase connection closed");
    }

    @Override public void onError(Exception ex) { ex.printStackTrace(); }
}