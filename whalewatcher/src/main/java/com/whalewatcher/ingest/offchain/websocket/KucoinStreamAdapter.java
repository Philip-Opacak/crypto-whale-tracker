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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/* KuCoin requires a dynamic WebSocket connection URL that is obtained via a REST call (bullet-public)
 * before connecting. This adapter first fetches a temporary token and server endpoint,
 * then creates a WebSocket client using that URL.
 */
@Component
public class KucoinStreamAdapter implements ExchangeStreamer {

    private static final Gson GSON = new Gson();

    // REST DTOs
    record BulletResp(String code, BulletData data) {}

    record BulletData(String token, List<InstanceServer> instanceServers) {}

    record InstanceServer(String endpoint, long pingInterval, long pingTimeout) {}

    // DTOs (Trade channel)
    record KucoinSubscribeMsg(String id, String type, String topic, boolean response) {}
    record KucoinPingMsg(String id, String type) {}

    record KucoinMsg(
            String id,
            String topic,
            String type,     // type
            String subject,  // subject
            KucoinTradeData data
    ) {}

    record KucoinTradeData(
            String makerOrderId,
            String price,
            String sequence,
            String side,        // buy/sell
            String size,
            String symbol,      // ticker
            String takerOrderId,
            String time,        // timestamp
            String tradeId,
            String type
    ) {}

    // Adapter fields
    private final IngestionService ingestionService;

    private final ExecutorService processingPool =
            Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    private volatile KucoinClient client;

    public KucoinStreamAdapter(IngestionService ingestionService) {
        this.ingestionService = ingestionService;
    }

    @Override
    public Exchange exchange() { return Exchange.KUCOIN; }

    @Override
    public void start() {
        try {
            BulletData bullet = fetchBulletPublic();
            InstanceServer server = bullet.instanceServers().get(0);

            String wsUrl = server.endpoint()
                    + "?token=" + URLEncoder.encode(bullet.token(), StandardCharsets.UTF_8)
                    + "&connectId=" + UUID.randomUUID().toString().replace("-", "");

            // create a fresh WS client with the dynamic URL
            client = new KucoinClient(
                    URI.create(wsUrl),
                    ingestionService,
                    processingPool,
                    server.pingInterval()
            );

            client.connect();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void stop() {
        try { if (client != null) client.close(); } catch (Exception ignored) {}
        processingPool.shutdownNow();
    }

    private BulletData fetchBulletPublic() throws Exception {
        var http = java.net.http.HttpClient.newHttpClient();

        var request = java.net.http.HttpRequest.newBuilder()
                .uri(URI.create("https://api.kucoin.com/api/v1/bullet-public"))
                .POST(java.net.http.HttpRequest.BodyPublishers.noBody())
                .build();

        var response = http.send(request, java.net.http.HttpResponse.BodyHandlers.ofString());

        BulletResp parsed = GSON.fromJson(response.body(), BulletResp.class);

        if (parsed == null || parsed.data() == null
                || parsed.data().token() == null
                || parsed.data().instanceServers() == null
                || parsed.data().instanceServers().isEmpty()) {
            throw new IllegalStateException("Invalid KuCoin bullet-public response: " + response.body());
        }

        return parsed.data();
    }

    // Inner WS client
    static class KucoinClient extends WebSocketClient {

        private static final Gson GSON = new Gson();

        private final IngestionService ingestionService;
        private final ExecutorService processingPool;
        private final ScheduledExecutorService heartbeat = Executors.newSingleThreadScheduledExecutor();

        private final long pingIntervalMs;
        private volatile boolean subscribed = false;

        KucoinClient(
                URI serverUri,
                IngestionService ingestionService,
                ExecutorService processingPool,
                long pingIntervalMs
        ) {
            super(serverUri);
            this.ingestionService = ingestionService;
            this.processingPool = processingPool;
            this.pingIntervalMs = pingIntervalMs > 0 ? pingIntervalMs : 18_000;
        }

        @Override
        public void onOpen(ServerHandshake handshake) {
            System.out.println("KuCoin opened (waiting for welcome...)");
            startHeartbeat();
        }

        @Override
        public void onMessage(String raw) {
            processingPool.submit(() -> {
                try {
                    KucoinMsg msg = GSON.fromJson(raw, KucoinMsg.class);
                    if (msg == null || msg.type() == null) return;

                    // Subscribe after welcome
                    if (!subscribed && "welcome".equalsIgnoreCase(msg.type())) {
                        subscribed = true;
                        sendSubscribeForFiveSymbols();
                        return;
                    }

                    // Ignore non-trade messages
                    if (!"message".equalsIgnoreCase(msg.type())) return;
                    if (!"trade.l3match".equalsIgnoreCase(msg.subject())) return;
                    if (msg.data() == null) return;

                    KucoinTradeData t = msg.data();

                    long timeMs = parseKucoinTimeToMillis(t.time());

                    Trade trade = new Trade(
                            Exchange.KUCOIN,
                            t.symbol(),
                            Double.parseDouble(t.price()),
                            Double.parseDouble(t.size()),
                            t.side() == null ? null : t.side().toLowerCase(Locale.ROOT),
                            timeMs
                    );

                    ingestionService.ingest(trade);

                } catch (Exception e) {
                    System.err.println("KuCoin parse error: " + e.getMessage());
                }
            });
        }

        // Send tickers of interest
        private void sendSubscribeForFiveSymbols() {
            String topic = "/market/match:" +
                    "BTC-USDT," +
                    "ETH-USDT," +
                    "BNB-USDT," +
                    "SOL-USDT," +
                    "XRP-USDT";

            KucoinSubscribeMsg sub = new KucoinSubscribeMsg(
                    String.valueOf(System.currentTimeMillis()),
                    "subscribe",
                    topic,
                    true
            );

            this.send(GSON.toJson(sub));
            System.out.println("KuCoin subscribed: " + topic);
        }

        private void startHeartbeat() {
            heartbeat.scheduleAtFixedRate(() -> {
                try {
                    KucoinPingMsg ping = new KucoinPingMsg(
                            String.valueOf(System.currentTimeMillis()),
                            "ping"
                    );
                    this.send(GSON.toJson(ping));
                } catch (Exception ignored) {}
            }, pingIntervalMs, pingIntervalMs, TimeUnit.MILLISECONDS);
        }

        // convert to time to ms
        private static long parseKucoinTimeToMillis(String timeStr) {
            if (timeStr == null) return System.currentTimeMillis();
            long v = Long.parseLong(timeStr);
            return (v > 1_000_000_000_000_000L) ? (v / 1_000_000L) : v;
        }

        @Override
        public void onClose(int code, String reason, boolean remote) {
            System.out.println("KuCoin closed: " + code + " " + reason);
            subscribed = false;
            try { heartbeat.shutdownNow(); } catch (Exception ignored) {}
        }

        @Override
        public void onError(Exception ex) {
            ex.printStackTrace();
        }
    }
}