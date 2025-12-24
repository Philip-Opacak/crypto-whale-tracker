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
import java.util.concurrent.*;

@Component
public class BitgetStreamAdapter extends WebSocketClient implements ExchangeStreamer {

    private static final Gson GSON = new Gson();

    /* Bitget requires heartbeats every 30 seconds to keep the connection
     * otherwise the connection will be closed
     */
    private final ScheduledExecutorService heartbeat =
            Executors.newSingleThreadScheduledExecutor();

    private volatile ScheduledFuture<?> heartbeatTask;

    record BitgetArg(
            String instType,   // "SPOT"
            String channel,    // "trade"
            String instId      // ticker
    ) {}

    // One trade entry inside "data"
    record BitgetTrade(
            String ts,         // timestamp (ms)
            String price,      // price
            String size,       // volume
            String side,       // side
            String tradeId
    ) {}

    // Trade push message
    record BitgetTradeMsg(
            String action,           // "snapshot" or "update"
            BitgetArg arg,
            java.util.List<BitgetTrade> data,
            Long ts                  // timestamp
    ) {}

    // Subscribe/unsubscribe ack messages
    record BitgetEventMsg(
            String event,
            BitgetArg arg,
            String code,
            String msg
    ) {}

    private final IngestionService ingestionService;
    private final ExecutorService processingPool =
            Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    public BitgetStreamAdapter(IngestionService ingestionService) {
        super(URI.create("wss://ws.bitget.com/v2/ws/public"));
        this.ingestionService = ingestionService;
    }

    @Override
    public Exchange exchange() {
        return Exchange.BITGET;
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

        if (heartbeatTask != null) heartbeatTask.cancel(true);
        heartbeat.shutdownNow();
    }

    @Override
    public void onOpen(ServerHandshake handshake) {
        String subscribe = """
        {
          "op": "subscribe",
          "args": [
            {"instType":"SPOT","channel":"trade","instId":"BTCUSDT"},
            {"instType":"SPOT","channel":"trade","instId":"ETHUSDT"},
            {"instType":"SPOT","channel":"trade","instId":"SOLUSDT"},
            {"instType":"SPOT","channel":"trade","instId":"XRPUSDT"},
            {"instType":"SPOT","channel":"trade","instId":"BNBUSDT"}
          ]
        }
        """;
        send(subscribe);

        // Send ping (heartbeat) to keep connection
        if (heartbeatTask != null) heartbeatTask.cancel(true);

        heartbeatTask = heartbeat.scheduleAtFixedRate(() -> {
            try { send("ping"); } catch (Exception ignored) {}
        }, 25, 25, TimeUnit.SECONDS);
    }

    @Override
    public void onMessage(String s) {
        // Bitget heartbeat response
        if (s == null || s.isBlank()) return;
        if ("pong".equalsIgnoreCase(s.trim())) return;

        processingPool.submit(() -> {
            try {
                // First try to parse as a trade push
                BitgetTradeMsg tradeMsg = GSON.fromJson(s, BitgetTradeMsg.class);

                if (tradeMsg != null
                        && tradeMsg.arg() != null
                        && "trade".equalsIgnoreCase(tradeMsg.arg().channel())
                        && "SPOT".equalsIgnoreCase(tradeMsg.arg().instType())
                        && tradeMsg.data() != null
                        && !tradeMsg.data().isEmpty()) {

                    for (BitgetTrade t : tradeMsg.data()) {
                        if (t == null) continue;
                        if (tradeMsg.arg().instId() == null || tradeMsg.arg().instId().isBlank()) continue;
                        if (t.price() == null || t.size() == null || t.ts() == null) continue;
                        if (t.price().isBlank() || t.size().isBlank() || t.ts().isBlank()) continue;

                        String side = t.side() == null ? null : t.side().toLowerCase(Locale.ROOT);
                        if (side != null && !side.equals("buy") && !side.equals("sell")) side = null;

                        Trade trade = new Trade(
                                exchange(),
                                tradeMsg.arg().instId(),
                                Double.parseDouble(t.price()),
                                Double.parseDouble(t.size()),
                                side,
                                Long.parseLong(t.ts())
                        );

                        ingestionService.ingest(trade);
                    }

                    return;
                }

                // Otherwise parse as an event/ack
                BitgetEventMsg evt = GSON.fromJson(s, BitgetEventMsg.class);
                if (evt != null && evt.event() != null) {
                    if ("error".equalsIgnoreCase(evt.event())) {
                        System.err.println("Bitget WS error: " + evt.code() + " " + evt.msg());
                    }
                }
            } catch (Exception e) {
                System.err.println("Bitget parse error: " + e.getMessage());
            }
        });
    }

    @Override
    public void onClose(int i, String s, boolean b) {
        System.out.println("Bitget connection closed");
        if (heartbeatTask != null) heartbeatTask.cancel(true);
    }

    @Override
    public void onError(Exception e) {
        e.printStackTrace();
    }
}