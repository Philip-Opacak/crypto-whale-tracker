package com.whalewatcher.ingest.offchain.websocket;

import com.whalewatcher.domain.Exchange;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.springframework.stereotype.Component;

import java.net.URI;

@Component
public class BinanceStreamAdapter extends WebSocketClient implements ExchangeStreamer {

    private final RawWsBus bus;

    public BinanceStreamAdapter(RawWsBus bus) {
        super(URI.create(
                "wss://stream.binance.com:9443/stream?streams=" +
                        "btcusdt@trade/" +
                        "ethusdt@trade/" +
                        "bnbusdt@trade/" +
                        "solusdt@trade/" +
                        "xrpusdt@trade"
        ));
        this.bus = bus;
    }

    @Override
    public Exchange exchange() { return Exchange.BINANCE; }

    @Override
    public void start() { this.connect(); }

    @Override
    public void stop() {
        try { this.close(); } catch (Exception ignored) {}
    }

    @Override
    public void onOpen(ServerHandshake handshake) {
        System.out.println("Binance connection opened");
    }

    @Override
    public void onMessage(String msg) {
        if (msg == null || msg.isBlank()) return;
        bus.publish(Exchange.BINANCE, msg);
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        System.out.println("Binance connection closed: " + code + " " + reason);
    }

    @Override
    public void onError(Exception ex) {
        ex.printStackTrace();
    }
}