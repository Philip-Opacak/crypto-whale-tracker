package com.whalewatcher.ingest.offchain.websocket;

import com.whalewatcher.domain.Exchange;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.springframework.stereotype.Component;

import java.net.URI;

@Component
public class BybitStreamAdapter extends WebSocketClient implements ExchangeStreamer {

    private final RawWsBus bus;

    public BybitStreamAdapter(RawWsBus bus) {
        super(URI.create("wss://stream.bybit.com/v5/public/spot"));
        this.bus = bus;
    }

    @Override
    public Exchange exchange() {
        return Exchange.BYBIT;
    }

    @Override
    public void start() {
        this.connect();
    }

    @Override
    public void stop() {
        try { this.close(); } catch (Exception ignored) {}
    }

    // Send subscription message to Bybit stream
    @Override
    public void onOpen(ServerHandshake handshake) {
        String subscribeMessage = """
        {
          "op": "subscribe",
          "args": [
            "publicTrade.BTCUSDT",
            "publicTrade.ETHUSDT",
            "publicTrade.SOLUSDT",
            "publicTrade.XRPUSDT",
            "publicTrade.BNBUSDT"
          ]
        }
        """;
        send(subscribeMessage);
        System.out.println("Bybit connection opened + subscribed");
    }

    @Override
    public void onMessage(String raw) {
        if (raw == null || raw.isBlank()) return;
        bus.publish(Exchange.BYBIT, raw);
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        System.out.println("Bybit connection closed: " + code + " " + reason);
    }

    @Override
    public void onError(Exception e) {
        e.printStackTrace();
    }
}