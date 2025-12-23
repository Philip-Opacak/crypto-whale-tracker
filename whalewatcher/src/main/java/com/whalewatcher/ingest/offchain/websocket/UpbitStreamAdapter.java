package com.whalewatcher.ingest.offchain.websocket;

import com.google.gson.Gson;
import com.whalewatcher.domain.Exchange;
import com.whalewatcher.domain.Trade;
import com.whalewatcher.service.IngestionService;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class UpbitStreamAdapter extends WebSocketClient implements ExchangeStreamer{

    private static final Gson GSON = new Gson();

    record UpbitTrade(
            String type,    // trade type
            String code,    // ticker
            Double trade_price, //price
            Double trade_volume,    //volume
            String ask_bid, //asl/bid
            Long trade_timestamp    //timestamp (ms)
    ) {}

    private final IngestionService ingestionService;
    private final ExecutorService processingPool =
            Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    public UpbitStreamAdapter(IngestionService ingestionService) {
        super(URI.create("wss://api.upbit.com/websocket/v1"));
        this.ingestionService = ingestionService;
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
        try{
            this.close();
        }catch (Exception e){}
        processingPool.shutdownNow();
    }

    @Override
    public void onOpen(ServerHandshake serverHandshake) {
        String subscribe = """
        [
          {"ticket":"whalewatcher-upbit"},
          {"type":"trade","codes":["USDT-BTC","USDT-ETH","USDT-SOL","USDT-XRP"]}
        ]
        """;
        send(subscribe);
    }
    // Decode binary frame from bybit then send UTF-8 JSON message to onMessage() that takes string input parameter
    @Override
    public void onMessage(ByteBuffer bytes) {
        if (bytes == null) return;
        byte[] arr = new byte[bytes.remaining()];
        bytes.get(arr);
        onMessage(new String(arr, java.nio.charset.StandardCharsets.UTF_8));
    }

    @Override
    public void onMessage(String s) {
        processingPool.submit(() -> {
            try {
                UpbitTrade t = GSON.fromJson(s, UpbitTrade.class);
                if (t == null) return;
                if (!"trade".equalsIgnoreCase(t.type())) return;
                if (t.code() == null || t.trade_price() == null ||
                        t.trade_volume() == null || t.trade_timestamp() == null) return;

                Trade trade = new Trade(
                        exchange(),
                        t.code(),
                        t.trade_price(),
                        t.trade_volume(),
                        normalizeSide(t.ask_bid()),
                        t.trade_timestamp()
                );

                ingestionService.ingest(trade);

            } catch (Exception e) {
                System.err.println("Upbit parse error: " + e.getMessage());
            }
        });
    }

    // Bid get converted to buy and ask to sell to support side enum structure
    private String normalizeSide(String ab) {
        if (ab == null) return null;
        return switch (ab.toUpperCase(Locale.ROOT)) {
            case "BID" -> "buy";
            case "ASK" -> "sell";
            default -> null;
        };
    }

    @Override
    public void onClose(int i, String s, boolean b) {
        System.out.println("Upbit connection closed");
    }

    @Override
    public void onError(Exception e) {
        e.printStackTrace();
    }
}