package com.whalewatcher.ingest.offchain.websocket;

import com.whalewatcher.domain.Exchange;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.springframework.stereotype.Component;

import java.net.URI;

@Component
public class GateStreamAdapter extends WebSocketClient implements ExchangeStreamer {

    private final RawWsBus bus;

    public GateStreamAdapter(RawWsBus bus) {
        super(URI.create("wss://api.gateio.ws/ws/v4/"));
        this.bus = bus;
    }

    @Override
    public Exchange exchange() {
        return Exchange.GATE;
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
           "time": 1700000000,
           "channel": "spot.trades",
           "event": "subscribe",
           "payload": ["BTC_USDT","ETH_USDT","SOL_USDT","XRP_USDT","BNB_USDT"]
        }
        """;
        send(subscribe);
        System.out.println("Gate.io connection opened + subscribed");
    }

    @Override
    public void onMessage(String raw) {
        if (raw == null || raw.isBlank()) return;
        bus.publish(Exchange.GATE, raw);
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        System.out.println("Gate stream closed: " + code + " " + reason);
    }

    @Override
    public void onError(Exception e) {
        e.printStackTrace();
    }
}