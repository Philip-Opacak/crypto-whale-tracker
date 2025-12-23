package com.whalewatcher.ingest.offchain.websocket;

import com.google.gson.Gson;
import com.whalewatcher.domain.Exchange;
import com.whalewatcher.domain.Trade;
import com.whalewatcher.service.IngestionService;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class BybitStreamAdapter extends WebSocketClient implements ExchangeStreamer{

    private static final Gson GSON = new Gson();

    record BybitTrade(
            String p,   // price
            String v,   // volume
            String S,   // side: Buy / Sell
            long T      // timestamp (ms)
    ) {}

    // trades (data) from bybit messages are provided as lists that will be iterated through
    record BybitMsg(
            String topic,   // bybit topics provide the trade ticker
            List<BybitTrade> data
    ) {}

    private final IngestionService ingestionService;

    private final ExecutorService processingPool =
            Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    public BybitStreamAdapter(IngestionService ingestionService) {
        super(URI.create("wss://stream.bybit.com/v5/public/spot"));
        this.ingestionService = ingestionService;
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
        try{
            this.close();
        }catch (Exception e){
            e.printStackTrace();
        }
        processingPool.shutdownNow();
    }

    // Send subscription message to bybit stream
    @Override
    public void onOpen(ServerHandshake serverHandshake) {
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
    }

    @Override
    public void onMessage(String s) {
        processingPool.submit(() -> {
            try {
                BybitMsg parsed = GSON.fromJson(s, BybitMsg.class);
                if (parsed == null || parsed.data() == null) return;

                // Iterate through trades
                for (BybitTrade t : parsed.data()) {
                    if (t == null) continue;
                    if (t.p() == null || t.v() == null) continue;

                    Trade trade = new Trade(
                            exchange(),
                            extractSymbol(parsed.topic()),
                            Double.parseDouble(t.p()),
                            Double.parseDouble(t.v()),
                            t.S() == null
                                    ? null
                                    : t.S().toLowerCase(Locale.ROOT),
                            t.T()
                    );

                    ingestionService.ingest(trade);
                }

            } catch (Exception e) {
                System.err.println("Bybit parse error: " + e.getMessage());
            }
        });
    }

    /* Topics in bybit or formatted publicTrade.BTCUSDT.
     * extractSymbol() takes the substring of the actual ticker in focus.
     */
    private String extractSymbol(String topic) {
        if (topic == null || !topic.contains(".")) return null;
        return topic.substring(topic.indexOf('.') + 1);
    }

    @Override
    public void onClose(int i, String s, boolean b) {
        System.out.println("Bybit connection closed");
    }

    @Override
    public void onError(Exception e) {
        e.printStackTrace();
    }
}
