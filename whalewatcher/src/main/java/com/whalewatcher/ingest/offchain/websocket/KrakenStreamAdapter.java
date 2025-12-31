package com.whalewatcher.ingest.offchain.websocket;

import com.whalewatcher.domain.Exchange;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.springframework.stereotype.Component;

import java.net.URI;

@Component
public class KrakenStreamAdapter extends WebSocketClient implements ExchangeStreamer {

    private final RawWsBus bus;

    public KrakenStreamAdapter(RawWsBus bus) {
        super(URI.create("wss://ws.kraken.com/v2"));
        this.bus = bus;
    }

    @Override
    public Exchange exchange() {
        return Exchange.KRAKEN;
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
        System.out.println("Kraken connection opened + subscribed");
    }

    @Override
    public void onMessage(String raw) {
        if (raw == null || raw.isBlank()) return;
        bus.publish(Exchange.KRAKEN, raw);
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        System.out.println("Kraken connection closed: " + code + " " + reason);
    }

    @Override
    public void onError(Exception ex) {
        ex.printStackTrace();
    }
}