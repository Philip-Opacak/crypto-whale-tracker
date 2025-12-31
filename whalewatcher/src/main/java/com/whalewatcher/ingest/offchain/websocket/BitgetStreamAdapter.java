package com.whalewatcher.ingest.offchain.websocket;

import com.whalewatcher.domain.Exchange;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.concurrent.*;

@Component
public class BitgetStreamAdapter extends WebSocketClient implements ExchangeStreamer {

    // Bitget requires heartbeats every 30 seconds to keep the connection otherwise the connection will be closed
    private final ScheduledExecutorService heartbeat =
            Executors.newSingleThreadScheduledExecutor();

    private volatile ScheduledFuture<?> heartbeatTask;

    private final RawWsBus bus;

    public BitgetStreamAdapter(RawWsBus bus) {
        super(URI.create("wss://ws.bitget.com/v2/ws/public"));
        this.bus = bus;
    }

    @Override
    public Exchange exchange() {
        return Exchange.BITGET;
    }

    @Override
    public void start() {
        this.connect();
    }

    @Override
    public void stop() {
        try { this.close(); } catch (Exception ignored) {}

        if (heartbeatTask != null) heartbeatTask.cancel(true);
        heartbeat.shutdownNow();
    }

    @Override
    public void onOpen(ServerHandshake handshake) {
        String subscribe = """
        {
          "op": "subscribe",
          "args": [
            {"instType":"SPOT","channel":"trade","instId":"BTCUSDT"},
            {"instType":"SPOT","channel":"trade","instId":"ETHUSDT"},
            {"instType":"SPOT","channel":"trade","instId":"SOLUSDT"},
            {"instType":"SPOT","channel":"trade","instId":"XRPUSDT"},
            {"instType":"SPOT","channel":"trade","instId":"BNBUSDT"}
          ]
        }
        """;
        send(subscribe);

        // Send ping (heartbeat) to keep connection alive
        if (heartbeatTask != null) heartbeatTask.cancel(true);

        heartbeatTask = heartbeat.scheduleAtFixedRate(() -> {
            try { send("ping"); } catch (Exception ignored) {}
        }, 25, 25, TimeUnit.SECONDS);
    }

    @Override
    public void onMessage(String raw) {
        if (raw == null || raw.isBlank()) return;
        // Bitget heartbeat response, no enqueue
        if ("pong".equalsIgnoreCase(raw.trim())) return;

        bus.publish(Exchange.BITGET, raw);
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        System.out.println("Bitget connection closed: " + code + " " + reason);
        if (heartbeatTask != null) heartbeatTask.cancel(true);
    }

    @Override
    public void onError(Exception e) {
        e.printStackTrace();
    }
}