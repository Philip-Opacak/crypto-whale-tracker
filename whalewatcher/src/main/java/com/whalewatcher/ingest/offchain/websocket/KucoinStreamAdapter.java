package com.whalewatcher.ingest.offchain.websocket;

import com.google.gson.Gson;
import com.whalewatcher.domain.Exchange;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.List;
import java.util.concurrent.Executors;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
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

    private final RawWsBus bus;

    private volatile KucoinClient client;

    public KucoinStreamAdapter(RawWsBus bus) {
        this.bus = bus;
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

            client = new KucoinClient(
                    URI.create(wsUrl),
                    bus,
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

        // DTOs
        record KucoinSubscribeMsg(String id, String type, String topic, boolean response) {}
        record KucoinPingMsg(String id, String type) {}

        record KucoinMsg(String type, String subject) {}

        private final RawWsBus bus;
        private final ScheduledExecutorService heartbeat =
                Executors.newSingleThreadScheduledExecutor();

        private final long pingIntervalMs;
        private volatile boolean subscribed = false;

        KucoinClient(URI serverUri, RawWsBus bus, long pingIntervalMs) {
            super(serverUri);
            this.bus = bus;
            this.pingIntervalMs = pingIntervalMs > 0 ? pingIntervalMs : 18_000;
        }

        @Override
        public void onOpen(ServerHandshake handshake) {
            System.out.println("KuCoin opened (waiting for welcome...)");
            startHeartbeat();
        }

        @Override
        public void onMessage(String raw) {
            if (raw == null || raw.isBlank()) return;

            // detect welcome then subscribe
            try {
                KucoinMsg msg = GSON.fromJson(raw, KucoinMsg.class);
                if (msg != null && msg.type() != null) {
                    if (!subscribed && "welcome".equalsIgnoreCase(msg.type())) {
                        subscribed = true;
                        sendSubscribeForFiveSymbols();
                        return;
                    }
                    // Only forward trade messages to workers
                    if (!"message".equalsIgnoreCase(msg.type())) return;
                    if (!"trade.l3match".equalsIgnoreCase(msg.subject())) return;

                    bus.publish(Exchange.KUCOIN, raw);
                }
            } catch (Exception ignored) {}
        }

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