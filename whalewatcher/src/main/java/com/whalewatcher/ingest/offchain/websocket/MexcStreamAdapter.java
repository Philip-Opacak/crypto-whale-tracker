package com.whalewatcher.ingest.offchain.websocket;

import com.google.gson.Gson;
import com.whalewatcher.domain.Exchange;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
public class MexcStreamAdapter extends WebSocketClient implements ExchangeStreamer {

    private static final Gson GSON = new Gson();

    // DTOs
    record DealParam(String symbol, Boolean compress) {}
    record SubDealMsg(String method, DealParam param) {}

    private final RawWsBus bus;

    // scheduler for delayed / spaced subscribe sends
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();

    public MexcStreamAdapter(RawWsBus bus) {
        super(URI.create("wss://contract.mexc.com/edge"));
        this.bus = bus;
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
        scheduler.shutdownNow();
    }

    @Override
    public void onOpen(ServerHandshake handshake) {
        System.out.println("MEXC connection opened");

        List<String> symbols = List.of("BTC_USDT", "ETH_USDT", "BNB_USDT", "SOL_USDT", "XRP_USDT");

        // MEXC can be disconnect if messages are sent too fast
        // Start after 250ms, then send one subscription every 50ms
        final long startDelayMs = 250;
        final long stepDelayMs = 50;

        for (int i = 0; i < symbols.size(); i++) {
            String sym = symbols.get(i);
            long delay = startDelayMs + (i * stepDelayMs);

            scheduler.schedule(() -> {
                try {
                    SubDealMsg sub = new SubDealMsg("sub.deal", new DealParam(sym, false));
                    this.send(GSON.toJson(sub));
                } catch (Exception e) {
                    System.err.println("MEXC subscribe error (" + sym + "): " + e.getMessage());
                }
            }, delay, TimeUnit.MILLISECONDS);
        }

        System.out.println("MEXC subscribing: " + symbols);
    }

    @Override
    public void onMessage(String raw) {
        if (raw == null || raw.isBlank()) return;
        bus.publish(Exchange.MEXC, raw);
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