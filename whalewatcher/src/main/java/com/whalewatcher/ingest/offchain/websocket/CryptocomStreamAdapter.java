package com.whalewatcher.ingest.offchain.websocket;

import com.google.gson.Gson;
import com.whalewatcher.domain.Exchange;
import com.whalewatcher.domain.Trade;
import com.whalewatcher.service.IngestionService;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class CryptocomStreamAdapter extends WebSocketClient implements ExchangeStreamer {

    private static final Gson GSON = new Gson();

    // DTOs
    record MethodOnly(String method, long id) {}

    // Heartbeat response
    record RespondHeartbeat(long id, String method) {}

    // Subscribe request
    record SubscribeReq(long id, String method, SubscribeParams params) {}
    record SubscribeParams(List<String> channels) {}

    // Generic inbound message
    record CryptoMsg(long id, String method, int code, TradeResult result) {}

    record TradeResult(
            String channel,                // "trade" channel is used
            String instrument_name,         // ticker
            String subscription,
            List<CryptoTrade> data          // snapshot (50) then incremental
    ) {}

    record CryptoTrade(
            String d,   // id
            long t,     // timestamp (ms)
            String p,   // price
            String q,   // quantity
            String s,   // BUY/SELL
            String i    // ticker
    ) {}

    // Fields
    private final IngestionService ingestionService;

    private final ExecutorService processingPool =
            Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    public CryptocomStreamAdapter(IngestionService ingestionService) {
        super(URI.create("wss://stream.crypto.com/v2/market"));
        this.ingestionService = ingestionService;
    }

    @Override
    public Exchange exchange() {
        return Exchange.CRYPTOCOM;
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

    @Override
    public void onOpen(ServerHandshake serverHandshake) {
        System.out.println("Crypto.com connection opened");

        // ~1s delay before sending requests to avoid TOO_MANY_REQUESTS error
        processingPool.submit(() -> {
            try { Thread.sleep(1000); } catch (InterruptedException ignored) {}

            List<String> channels = List.of(
                    "trade.BTCUSD-PERP",
                    "trade.ETHUSD-PERP",
                    "trade.SOLUSD-PERP",
                    "trade.XRPUSD-PERP",
                    "trade.BNBUSD-PERP"
            );

            long reqId = System.currentTimeMillis(); // unique-ish per connection
            SubscribeReq sub = new SubscribeReq(reqId, "subscribe", new SubscribeParams(channels));

            this.send(GSON.toJson(sub));
            System.out.println("Crypto.com subscribed: " + channels);
        });
    }

    @Override
    public void onMessage(String raw) {
        processingPool.submit(() -> {
            try {
                // respond to heartbeat
                MethodOnly m = GSON.fromJson(raw, MethodOnly.class);
                if (m != null && m.method() != null && "public/heartbeat".equalsIgnoreCase(m.method())) {
                    RespondHeartbeat resp = new RespondHeartbeat(m.id(), "public/respond-heartbeat");
                    this.send(GSON.toJson(resp));
                    return;
                }

                // Trades and subscribe acks
                CryptoMsg msg = GSON.fromJson(raw, CryptoMsg.class);
                if (msg == null || msg.method() == null) return;

                // Non-zero code indicates error for method/request
                if (msg.code() != 0) {
                    System.err.println("Crypto.com WS error. method=" + msg.method() + " code=" + msg.code());
                    return;
                }

                // Ignore initial subscribe ack
                if ("subscribe".equalsIgnoreCase(msg.method()) && msg.result() == null) return;

                // Channel pushes also come through method="subscribe"
                if (!"subscribe".equalsIgnoreCase(msg.method())) return;
                if (msg.result() == null) return;
                if (msg.result().channel() == null || !"trade".equalsIgnoreCase(msg.result().channel())) return;
                if (msg.result().data() == null || msg.result().data().isEmpty()) return;

                for (CryptoTrade t : msg.result().data()) {
                    if (t == null) continue;

                    Trade trade = new Trade(
                            exchange(),
                            t.i(), // ticker
                            Double.parseDouble(t.p()),
                            Double.parseDouble(t.q()),
                            t.s() == null ? null : t.s().toLowerCase(Locale.ROOT),
                            t.t()
                    );

                    ingestionService.ingest(trade);
                }

            } catch (Exception e) {
                System.err.println("Crypto.com parse error: " + e.getMessage());
            }
        });
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        System.out.println("Crypto.com connection closed: " + code + " " + reason);
    }

    @Override
    public void onError(Exception e) {
        e.printStackTrace();
    }
}