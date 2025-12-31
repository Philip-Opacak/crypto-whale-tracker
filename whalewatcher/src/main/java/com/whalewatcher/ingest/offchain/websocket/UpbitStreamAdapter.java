package com.whalewatcher.ingest.offchain.websocket;

import com.whalewatcher.domain.Exchange;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

@Component
public class UpbitStreamAdapter extends WebSocketClient implements ExchangeStreamer {

    private final RawWsBus bus;

    public UpbitStreamAdapter(RawWsBus bus) {
        super(URI.create("wss://api.upbit.com/websocket/v1"));
        this.bus = bus;
    }

    @Override
    public Exchange exchange() {
        return Exchange.UPBIT;
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
        [
          {"ticket":"whalewatcher-upbit"},
          {"type":"trade","codes":["USDT-BTC","USDT-ETH","USDT-SOL","USDT-XRP"]}
        ]
        """;
        send(subscribe);
        System.out.println("Upbit connection opened + subscribed");
    }

    // Upbit sends binary frames, decode to UTF-8 JSON and hand off to workers
    @Override
    public void onMessage(ByteBuffer bytes) {
        if (bytes == null) return;

        byte[] arr = new byte[bytes.remaining()];
        bytes.get(arr);

        String json = new String(arr, StandardCharsets.UTF_8);
        onMessage(json);
    }

    @Override
    public void onMessage(String raw) {
        if (raw == null || raw.isBlank()) return;
        bus.publish(Exchange.UPBIT, raw);
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        System.out.println("Upbit connection closed: " + code + " " + reason);
    }

    @Override
    public void onError(Exception e) {
        e.printStackTrace();
    }
}