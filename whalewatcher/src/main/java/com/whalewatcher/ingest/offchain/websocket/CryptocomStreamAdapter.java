package com.whalewatcher.ingest.offchain.websocket;

import com.google.gson.Gson;
import com.whalewatcher.domain.Exchange;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
public class CryptocomStreamAdapter extends WebSocketClient implements ExchangeStreamer {

    private static final Gson GSON = new Gson();

    // DTOs
    record MethodOnly(String method, long id) {}
    record RespondHeartbeat(long id, String method) {}

    record SubscribeReq(long id, String method, SubscribeParams params) {}
    record SubscribeParams(List<String> channels) {}

    private final RawWsBus bus;

    // scheduler for delayed subscribe (avoid TOO_MANY_REQUESTS error)
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();

    public CryptocomStreamAdapter(RawWsBus bus) {
        super(URI.create("wss://stream.crypto.com/v2/market"));
        this.bus = bus;
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
        scheduler.shutdownNow();
    }

    @Override
    public void onOpen(ServerHandshake handshake) {
        System.out.println("Crypto.com connection opened");

        // ~1s delay before sending requests to avoid TOO_MANY_REQUESTS error
        scheduler.schedule(() -> {
            try {
                List<String> channels = List.of(
                        "trade.BTCUSD-PERP",
                        "trade.ETHUSD-PERP",
                        "trade.SOLUSD-PERP",
                        "trade.XRPUSD-PERP",
                        "trade.BNBUSD-PERP"
                );

                long reqId = System.currentTimeMillis();
                SubscribeReq sub = new SubscribeReq(reqId, "subscribe", new SubscribeParams(channels));

                this.send(GSON.toJson(sub));
                System.out.println("Crypto.com subscribed: " + channels);
            } catch (Exception e) {
                System.err.println("Crypto.com subscribe error: " + e.getMessage());
            }
        }, 1, TimeUnit.SECONDS);
    }

    @Override
    public void onMessage(String raw) {
        if (raw == null || raw.isBlank()) return;

        // Respond to heartbeat (workers cannot call send()).
        try {
            MethodOnly m = GSON.fromJson(raw, MethodOnly.class);
            if (m != null && m.method() != null
                    && "public/heartbeat".equalsIgnoreCase(m.method())) {
                RespondHeartbeat resp = new RespondHeartbeat(m.id(), "public/respond-heartbeat");
                this.send(GSON.toJson(resp));
                return;
            }
        } catch (Exception ignored) {}
        bus.publish(Exchange.CRYPTOCOM, raw);
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