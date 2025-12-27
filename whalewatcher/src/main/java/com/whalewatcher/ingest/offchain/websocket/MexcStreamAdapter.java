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
public class MexcStreamAdapter extends WebSocketClient implements ExchangeStreamer {

    private static final Gson GSON = new Gson();

    // DTOs
    record DealParam(String symbol, Boolean compress) {}
    record SubDealMsg(String method, DealParam param) {}

    record DealPush(
            String symbol,  //ticker
            List<DealItem> data,
            String channel,
            long ts
    ) {}

    record DealItem(
            double p,   // price
            double v,   // volume
            int T,      // side
            int O,
            int M,
            long t      // timestamp (ms)
    ) {}

    private final IngestionService ingestionService;

    private final ExecutorService processingPool =
            Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    public MexcStreamAdapter(IngestionService ingestionService) {
        super(URI.create("wss://contract.mexc.com/edge"));
        this.ingestionService = ingestionService;
    }

    @Override
    public Exchange exchange() {
        return Exchange.MEXC;
    }

    @Override
    public void start() {
        this.connect();
    }

    @Override
    public void stop() {
        try { this.close(); } catch (Exception ignored) {}
        processingPool.shutdownNow();
    }

    @Override
    public void onOpen(ServerHandshake serverHandshake) {
        System.out.println("MEXC connection opened");

        // Send each ticker in a separate subscription message
        processingPool.submit(() -> {
            try { Thread.sleep(250); } catch (InterruptedException ignored) {}

            List<String> symbols = List.of("BTC_USDT", "ETH_USDT", "BNB_USDT", "SOL_USDT", "XRP_USDT");

            for (String sym : symbols) {
                SubDealMsg sub = new SubDealMsg("sub.deal", new DealParam(sym, false));
                this.send(GSON.toJson(sub));

                try { Thread.sleep(50); } catch (InterruptedException ignored) {}
            }

            System.out.println("MEXC subscribed: " + symbols);
        });
    }

    @Override
    public void onMessage(String s) {
        processingPool.submit(() -> {
            try {
                DealPush push = GSON.fromJson(s, DealPush.class);
                if (push == null) return;

                // Only focus on push messages
                if (push.channel() == null) return;
                if (!"push.deal".equalsIgnoreCase(push.channel())) return;

                if (push.symbol() == null) return;
                if (push.data() == null || push.data().isEmpty()) return;

                String symbol = push.symbol();

                for (DealItem d : push.data()) {
                    if (d == null) continue;

                    String side = switch (d.T) {
                        case 1 -> "buy";
                        case 2 -> "sell";
                        default -> null;
                    };

                    Trade trade = new Trade(
                            exchange(),
                            symbol,
                            d.p,
                            d.v,
                            side == null ? null : side.toLowerCase(Locale.ROOT),
                            d.t
                    );

                    ingestionService.ingest(trade);
                }

            } catch (Exception e) {
                System.err.println("MEXC parse error: " + e.getMessage());
            }
        });
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        System.out.println("MEXC connection closed: code=" + code + " reason=" + reason + " remote=" + remote);
    }

    @Override
    public void onError(Exception e) {
        e.printStackTrace();
    }
}