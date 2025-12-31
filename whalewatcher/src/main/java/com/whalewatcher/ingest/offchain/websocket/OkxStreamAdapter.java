package com.whalewatcher.ingest.offchain.websocket;

import com.whalewatcher.domain.Exchange;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.springframework.stereotype.Component;

import java.net.URI;

@Component
public class OkxStreamAdapter extends WebSocketClient implements ExchangeStreamer {

    private final RawWsBus bus;

    public OkxStreamAdapter(RawWsBus bus) {
        super(URI.create("wss://ws.okx.com:8443/ws/v5/public"));
        this.bus = bus;
    }

    @Override
    public Exchange exchange() {
        return Exchange.OKX;
    }

    @Override
    public void start() {
        this.connect();
    }

    @Override
    public void stop() {
        try { this.close(); } catch (Exception ignored) {}
    }

    @Override
    public void onOpen(ServerHandshake handshake) {
        String subscribe = """
        {
            "op": "subscribe",
            "args": [
                {"channel":"trades","instId":"BTC-USDT"},
                {"channel":"trades","instId":"ETH-USDT"},
                {"channel":"trades","instId":"SOL-USDT"},
                {"channel":"trades","instId":"XRP-USDT"},
                {"channel":"trades","instId":"BNB-USDT"}
            ]
        }
        """;
        send(subscribe);
        System.out.println("OKX connection opened + subscribed");
    }

    @Override
    public void onMessage(String raw) {
        if (raw == null || raw.isBlank()) return;
        bus.publish(Exchange.OKX, raw);
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        System.out.println("OKX connection closed: " + code + " " + reason);
    }

    @Override
    public void onError(Exception e) {
        e.printStackTrace();
    }
}