package com.whalewatcher.ingest.offchain.websocket;

import com.google.gson.Gson;
import com.whalewatcher.domain.Exchange;
import com.whalewatcher.domain.Trade;
import com.whalewatcher.service.IngestionService;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class BinanceStreamAdapter extends WebSocketClient implements ExchangeStreamer {

    private static final Gson GSON = new Gson();

    // The inner "data" object from Binance combined stream trade messages
    record BinanceTrade(
            String e,   // event type
            long E,     // event time (ms)
            String s,   // symbol
            String p,   // price
            String q,   // qty
            long T,     // trade time (ms)
            boolean m   // buyer is market maker
    ) {}

    // Wrapper for combined streams: { "stream": "btcusdt@trade", "data": { ... } }
    record BinanceMsg(String stream, BinanceTrade data) {}

    private final IngestionService ingestionService;

    private final ExecutorService processingPool =
            Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    public BinanceStreamAdapter(IngestionService ingestionService) {
        super(URI.create(
                "wss://stream.binance.com:9443/stream?streams=" +
                        "btcusdt@trade/" +
                        "ethusdt@trade/" +
                        "bnbusdt@trade/" +
                        "solusdt@trade/" +
                        "xrpusdt@trade"
        ));
        this.ingestionService = ingestionService;
    }

    @Override
    public Exchange exchange() { return Exchange.BINANCE; }

    @Override
    public void start() { this.connect(); }

    @Override
    public void stop() {
        try { this.close(); } catch (Exception ignored) {}
        processingPool.shutdownNow();
    }

    @Override
    public void onOpen(ServerHandshake handshake) {
        System.out.println("Binance connection opened");
    }

    @Override
    public void onMessage(String msg) {
        processingPool.submit(() -> {
            try {
                BinanceMsg parsed = GSON.fromJson(msg, BinanceMsg.class);
                if (parsed == null || parsed.data() == null) return;

                BinanceTrade data = parsed.data();

                // Only process trade events
                if (!"trade".equalsIgnoreCase(data.e())) return;

                Trade trade = new Trade(
                        exchange(),
                        data.s(),
                        Double.parseDouble(data.p()),
                        Double.parseDouble(data.q()),
                        //If true the buyer is the maker (seller is taker) making the side a sell
                        //If false the buyer is the taker (seller is maker) making the side a buy
                        data.m() ? "sell" : "buy",
                        data.T()
                );

                ingestionService.ingest(trade);

            } catch (Exception e) {
                System.err.println("Binance parse error: " + e.getMessage());
            }
        });
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        System.out.println("Binance connection closed");
    }

    @Override
    public void onError(Exception ex) { ex.printStackTrace(); }
}