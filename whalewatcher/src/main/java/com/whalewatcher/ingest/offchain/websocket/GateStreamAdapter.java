package com.whalewatcher.ingest.offchain.websocket;

import com.google.gson.Gson;
import com.whalewatcher.domain.Exchange;
import com.whalewatcher.domain.Trade;
import com.whalewatcher.service.IngestionService;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class GateStreamAdapter extends WebSocketClient implements ExchangeStreamer {

    private static final Gson GSON = new Gson();

    record GateTrade(
            Long id,
            Long id_market,
            Long create_time,
            String create_time_ms,   // ms
            String side,             // buy / sell
            String currency_pair,    // ticker
            String amount,           // size
            String price             // price
    ) {}

    record GateMsg(
            Long time,
            String channel,          // "spot.trades"
            String event,            // "update"
            GateTrade result
    ) {}

    private final IngestionService ingestionService;
    private final ExecutorService processingPool =
            Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    public GateStreamAdapter(IngestionService ingestionService) {
        super(URI.create("wss://api.gateio.ws/ws/v4/"));
        this.ingestionService = ingestionService;
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
           "time": 1700000000,
           "channel": "spot.trades",
           "event": "subscribe",
           "payload": ["BTC_USDT","ETH_USDT","SOL_USDT","XRP_USDT","BNB_USDT"]
        }
        """;
        send(subscribe);
    }

    @Override
    public void onMessage(String s) {
        if (s == null || s.isBlank()) return;

        processingPool.submit(() -> {
            try {
                GateMsg msg = GSON.fromJson(s, GateMsg.class);
                if (msg == null) return;

                // focus on trade updates from the public trades channel
                if (msg.channel() == null || !"spot.trades".equalsIgnoreCase(msg.channel())) return;
                if (msg.event() == null || !"update".equalsIgnoreCase(msg.event())) return;

                GateTrade t = msg.result();
                if (t == null) return;

                // field validation
                if (t.currency_pair() == null || t.currency_pair().isBlank()) return;
                if (t.price() == null || t.price().isBlank()) return;
                if (t.amount() == null || t.amount().isBlank()) return;

                // avoid floating point rounding
                long tsMs;
                if (t.create_time_ms() != null && !t.create_time_ms().isBlank()) {
                    String ms = t.create_time_ms().trim();
                    int dot = ms.indexOf('.');
                    if (dot >= 0) ms = ms.substring(0, dot);
                    tsMs = Long.parseLong(ms);
                } else if (t.create_time() != null) {
                    tsMs = t.create_time() * 1000L;
                } else {
                    return;
                }

                String side = t.side() == null ? null : t.side().toLowerCase(Locale.ROOT);
                if (side != null && !side.equals("buy") && !side.equals("sell")) side = null;

                Trade trade = new Trade(
                        exchange(),
                        t.currency_pair(),
                        Double.parseDouble(t.price()),
                        Double.parseDouble(t.amount()),
                        side,
                        tsMs
                );

                ingestionService.ingest(trade);

            } catch (Exception e) {
                System.err.println("Gate parse error: " + e.getMessage());
            }
        });
    }

    @Override
    public void onClose(int i, String s, boolean b) {
        System.out.println("Gate stream closed");
    }

    @Override
    public void onError(Exception e) {
        e.printStackTrace();
    }
}