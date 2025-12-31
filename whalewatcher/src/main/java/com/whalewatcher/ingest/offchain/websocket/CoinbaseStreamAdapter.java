package com.whalewatcher.ingest.offchain.websocket;

import com.whalewatcher.domain.Exchange;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.springframework.stereotype.Component;

import java.net.URI;

@Component
public class CoinbaseStreamAdapter extends WebSocketClient implements ExchangeStreamer {

    private final RawWsBus bus;

    public CoinbaseStreamAdapter(RawWsBus bus) {
        super(URI.create("wss://advanced-trade-ws.coinbase.com"));
        this.bus = bus;
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
    }

    // Sends subscription message requesting trade updates
    @Override
    public void onOpen(ServerHandshake handshake) {
        String subscribeTrades = """
        {
            "type": "subscribe",
            "product_ids": ["BTC-USD","ETH-USD","SOL-USD","XRP-USD"],
            "channel": "market_trades"
        }
        """;
        send(subscribeTrades);

        // Subscribe to heartbeats
        String subscribeHeartbeats = """
        {
            "type": "subscribe",
            "channel": "heartbeats"
        }
        """;
        send(subscribeHeartbeats);

        System.out.println("Coinbase connection opened + subscribed");
    }

    @Override
    public void onMessage(String raw) {
        if (raw == null || raw.isBlank()) return;
        bus.publish(Exchange.COINBASE, raw);
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        System.out.println("Coinbase connection closed: " + code + " " + reason);
    }

    @Override
    public void onError(Exception ex) {
        ex.printStackTrace();
    }
}