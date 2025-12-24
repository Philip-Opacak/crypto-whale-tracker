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
public class OkxStreamAdapter extends WebSocketClient implements ExchangeStreamer {
    private static final Gson GSON = new Gson();

    record OkxTrade(
            String instId,  // ticker
            String px,  // price
            String sz,  // volume
            String side,    // side
            String ts   // timestamp
    ) {}

    record OkxMsg(
            OkxArg arg,
            List<OkxTrade> data
    ) {}

    record OkxArg(
            String channel,
            String instId
    ) {}

    private final IngestionService ingestionService;
    private final ExecutorService processingPool =
            Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    public OkxStreamAdapter(IngestionService ingestionService) {
        super(URI.create("wss://ws.okx.com:8443/ws/v5/public"));
        this.ingestionService = ingestionService;
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
        try{
            this.close();
        }catch(Exception e){
            e.printStackTrace();
        }
        processingPool.shutdownNow();
    }

    @Override
    public void onOpen(ServerHandshake serverHandshake) {
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
    }

    @Override
    public void onMessage(String s) {
        processingPool.submit(() -> {
            try {
                OkxMsg msg = GSON.fromJson(s, OkxMsg.class);
                if (msg == null) return;

                // Ignore subscription acks / event messages they will not contain data
                if (msg.data() == null || msg.data().isEmpty()) return;

                // Ensure it is the trades channel
                if (msg.arg() == null || !"trades".equalsIgnoreCase(msg.arg().channel())) return;

                // Iterate through the trades
                for (OkxTrade t : msg.data()) {
                    if (t == null) continue;
                    if (t.instId() == null || t.px() == null || t.sz() == null || t.ts() == null) continue;
                    if (t.instId().isBlank() || t.px().isBlank() || t.sz().isBlank() || t.ts().isBlank()) continue;

                    // Suffix check
                    if (!t.instId().endsWith("-USDT")) continue;

                    String side = t.side() == null ? null : t.side().toLowerCase(Locale.ROOT);
                    if (side != null && !side.equals("buy") && !side.equals("sell")) side = null;

                    Trade trade = new Trade(
                            exchange(),
                            t.instId(),
                            Double.parseDouble(t.px()),
                            Double.parseDouble(t.sz()),
                            side,
                            Long.parseLong(t.ts())
                    );

                    ingestionService.ingest(trade);
                }

            } catch (Exception e) {
                System.err.println("OKX parse error: " + e.getMessage());
            }
        });
    }

    @Override
    public void onClose(int i, String s, boolean b) {
        System.out.println("OKX connection closed");
    }

    @Override
    public void onError(Exception e) {
        e.printStackTrace();
    }
}