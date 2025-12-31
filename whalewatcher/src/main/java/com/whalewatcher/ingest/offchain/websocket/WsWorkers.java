package com.whalewatcher.ingest.offchain.websocket;

import com.google.gson.Gson;
import com.whalewatcher.domain.Exchange;
import com.whalewatcher.domain.Trade;
import com.whalewatcher.service.IngestionService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.ArrayList;

@Component
public class WsWorkers {

    private static final Gson GSON = new Gson();

    private static final int WS_WORKERS =
            Math.max(2, Math.min(6, Runtime.getRuntime().availableProcessors() - 1));

    private final RawWsBus bus;
    private final IngestionService ingestionService;
    private final ExecutorService pool;

    private volatile boolean running = true;

    public WsWorkers(RawWsBus bus, IngestionService ingestionService) {
        this.bus = bus;
        this.ingestionService = ingestionService;

        this.pool = Executors.newFixedThreadPool(WS_WORKERS);
        System.out.println("WS workers started: " + WS_WORKERS);
    }

    @PostConstruct
    public void start() {
        for (int i = 0; i < WS_WORKERS; i++) {
            pool.submit(this::loop);
        }
    }

    private void loop() {
        while (running) {
            try {
                RawWsBus.RawWsMsg m = bus.take();

                List<Trade> trades = switch (m.exchange()) {
                    case KUCOIN -> parseKucoinTrades(m.raw());
                    case BINANCE -> parseBinanceTrades(m.raw());
                    case COINBASE -> parseCoinbaseTrades(m.raw());
                    case KRAKEN -> parseKrakenTrades(m.raw());
                    case BITGET -> parseBitgetTrades(m.raw());
                    case BYBIT -> parseBybitTrades(m.raw());
                    case CRYPTOCOM -> parseCryptoComTrades(m.raw());
                    case GATE -> parseGateTrades(m.raw());
                    case UPBIT -> parseUpbitTrades(m.raw());
                    case OKX -> parseOkxTrades(m.raw());
                    case MEXC ->  parseMexcTrades(m.raw());
                    default -> Collections.emptyList();
                };

                for (Trade t : trades) {
                    ingestionService.ingest(t);
                }

            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                System.err.println("WS worker error: " + e.getMessage());
            }
        }
    }

    @PreDestroy
    public void stop() {
        running = false;
        pool.shutdownNow();
    }

    private <T> T tryParse(String raw, Class<T> clazz) {
        try {
            return GSON.fromJson(raw, clazz);
        } catch (Exception ignored) {
            return null;
        }
    }

    // Kucoin parser
    record KucoinMsg(String type, String subject, KucoinTradeData data) {}
    record KucoinTradeData(String price, String side, String size, String symbol, String time) {}

    private List<Trade> parseKucoinTrades(String raw) {
        KucoinMsg msg = tryParse(raw, KucoinMsg.class);
        if (msg == null || msg.type() == null) return Collections.emptyList();

        if (!"message".equalsIgnoreCase(msg.type())) return Collections.emptyList();
        if (!"trade.l3match".equalsIgnoreCase(msg.subject())) return Collections.emptyList();
        if (msg.data() == null) return Collections.emptyList();

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

        return Collections.singletonList(trade);
    }

    private static long parseKucoinTimeToMillis(String timeStr) {
        if (timeStr == null) return System.currentTimeMillis();
        long v = Long.parseLong(timeStr);
        return (v > 1_000_000_000_000_000L) ? (v / 1_000_000L) : v;
    }

    // Binance parser
    record BinanceTrade(
            String e,   // event type
            long E,     // event time (ms)
            String s,   // symbol
            String p,   // price
            String q,   // qty
            long T,     // trade time (ms)
            boolean m   // buyer is market maker
    ) {}

    record BinanceMsg(String stream, BinanceTrade data) {}

    private List<Trade> parseBinanceTrades(String raw) {
        BinanceMsg parsed = tryParse(raw, BinanceMsg.class);
        if (parsed == null || parsed.data() == null) return Collections.emptyList();

        BinanceTrade data = parsed.data();

        if (!"trade".equalsIgnoreCase(data.e())) return Collections.emptyList();

        Trade trade = new Trade(
                Exchange.BINANCE,
                data.s(),
                Double.parseDouble(data.p()),
                Double.parseDouble(data.q()),
                data.m() ? "sell" : "buy",
                data.T()
        );

        return Collections.singletonList(trade);
    }

    // CoinBase parser
    record CoinbaseTrade(
            String trade_id,
            String product_id,
            String price,
            String size,
            String side,
            String time
    ) {}

    record CoinbaseEvent(
            String type,
            List<CoinbaseTrade> trades
    ) {}

    record CoinbaseMsg(
            String channel,
            String timestamp,
            List<CoinbaseEvent> events
    ) {}

    private List<Trade> parseCoinbaseTrades(String raw) {
        CoinbaseMsg parsed = tryParse(raw, CoinbaseMsg.class);
        if (parsed == null) return Collections.emptyList();

        if (!"market_trades".equals(parsed.channel())) return Collections.emptyList();
        if (parsed.events() == null || parsed.events().isEmpty()) return Collections.emptyList();

        List<Trade> out = new ArrayList<>(4);

        for (CoinbaseEvent evt : parsed.events()) {
            if (evt == null || evt.trades() == null) continue;

            for (CoinbaseTrade t : evt.trades()) {
                if (t == null) continue;
                if (t.price() == null || t.size() == null || t.time() == null || t.product_id() == null) continue;
                if (t.price().isBlank() || t.size().isBlank() || t.time().isBlank() || t.product_id().isBlank())
                    continue;

                out.add(new Trade(
                        Exchange.COINBASE,
                        t.product_id(),
                        Double.parseDouble(t.price()),
                        Double.parseDouble(t.size()),
                        t.side() == null ? null : t.side().toLowerCase(Locale.ROOT),
                        Instant.parse(t.time()).toEpochMilli()
                ));
            }
        }

        return out.isEmpty() ? Collections.emptyList() : out;
    }

    // Kraken parser
    record KrakenTrade(String symbol, String side, double price, double qty, String timestamp) {}
    record KrakenMsg(String channel, String type, List<KrakenTrade> data) {}

    private List<Trade> parseKrakenTrades(String raw) {
        KrakenMsg parsed = tryParse(raw, KrakenMsg.class);

        if (parsed == null) return Collections.emptyList();
        if (!"trade".equals(parsed.channel())) return Collections.emptyList();
        if (parsed.data() == null || parsed.data().isEmpty()) return Collections.emptyList();

        List<Trade> out = new ArrayList<>(parsed.data().size());

        for (KrakenTrade t : parsed.data()) {
            if (t == null) continue;

            out.add(new Trade(
                    Exchange.KRAKEN,
                    t.symbol(),
                    t.price(),
                    t.qty(),
                    t.side(),
                    Instant.parse(t.timestamp()).toEpochMilli()
            ));
        }

        return out.isEmpty() ? Collections.emptyList() : out;
    }


    // Bitget parser
    record BitgetArg(String instType, String channel, String instId) {}

    record BitgetTrade(
            String ts,
            String price,
            String size,
            String side,
            String tradeId
    ) {}

    record BitgetTradeMsg(
            String action,
            BitgetArg arg,
            List<BitgetTrade> data,
            Long ts
    ) {}

    private List<Trade> parseBitgetTrades(String s) {
        if (s == null || s.isBlank()) return Collections.emptyList();
        if ("pong".equalsIgnoreCase(s.trim())) return Collections.emptyList();

        BitgetTradeMsg tradeMsg = tryParse(s, BitgetTradeMsg.class);

        if (tradeMsg == null
                || tradeMsg.arg() == null
                || tradeMsg.data() == null
                || tradeMsg.data().isEmpty()) {
            return Collections.emptyList();
        }

        if (!"trade".equalsIgnoreCase(tradeMsg.arg().channel())) return Collections.emptyList();
        if (!"SPOT".equalsIgnoreCase(tradeMsg.arg().instType())) return Collections.emptyList();

        String instId = tradeMsg.arg().instId();
        if (instId == null || instId.isBlank()) return Collections.emptyList();

        List<Trade> out = new ArrayList<>(tradeMsg.data().size());

        for (BitgetTrade t : tradeMsg.data()) {
            if (t == null) continue;
            if (t.price() == null || t.size() == null || t.ts() == null) continue;
            if (t.price().isBlank() || t.size().isBlank() || t.ts().isBlank()) continue;

            String side = t.side() == null ? null : t.side().toLowerCase(Locale.ROOT);
            if (side != null && !side.equals("buy") && !side.equals("sell")) side = null;

            out.add(new Trade(
                    Exchange.BITGET,
                    instId,
                    Double.parseDouble(t.price()),
                    Double.parseDouble(t.size()),
                    side,
                    Long.parseLong(t.ts())
            ));
        }

        return out.isEmpty() ? Collections.emptyList() : out;
    }

    // Bybit parser
    record BybitTrade(
            String p,
            String v,
            String S,
            long T
    ) {}

    record BybitMsg(
            String topic,
            List<BybitTrade> data
    ) {}

    private List<Trade> parseBybitTrades(String s) {
        if (s == null || s.isBlank()) return Collections.emptyList();

        BybitMsg parsed = tryParse(s, BybitMsg.class);
        if (parsed == null || parsed.data() == null || parsed.data().isEmpty()) return Collections.emptyList();

        String symbol = extractBybitSymbol(parsed.topic());
        if (symbol == null) return Collections.emptyList();

        List<Trade> out = new ArrayList<>(parsed.data().size());

        for (BybitTrade t : parsed.data()) {
            if (t == null) continue;
            if (t.p() == null || t.v() == null) continue;
            if (t.p().isBlank() || t.v().isBlank()) continue;

            out.add(new Trade(
                    Exchange.BYBIT,
                    symbol,
                    Double.parseDouble(t.p()),
                    Double.parseDouble(t.v()),
                    t.S() == null ? null : t.S().toLowerCase(Locale.ROOT),
                    t.T()
            ));
        }

        return out.isEmpty() ? Collections.emptyList() : out;
    }

    private String extractBybitSymbol(String topic) {
        if (topic == null || topic.isBlank()) return null;

        int idx = topic.lastIndexOf('.');
        if (idx >= 0 && idx + 1 < topic.length()) {
            return topic.substring(idx + 1);
        }
        return topic;
    }

    // Crypto.com parser
    record CryptoMsg(long id, String method, int code, TradeResult result) {}

    record TradeResult(
            String channel,
            String instrument_name,
            String subscription,
            List<CryptoTrade> data
    ) {}

    record CryptoTrade(
            String d,
            long t,
            String p,
            String q,
            String s,
            String i
    ) {}

    private List<Trade> parseCryptoComTrades(String raw) {
        if (raw == null || raw.isBlank()) return Collections.emptyList();

        CryptoMsg msg = tryParse(raw, CryptoMsg.class);
        if (msg == null || msg.method() == null) return Collections.emptyList();

        if (msg.code() != 0) return Collections.emptyList();
        if (!"subscribe".equalsIgnoreCase(msg.method())) return Collections.emptyList();
        if (msg.result() == null) return Collections.emptyList();

        if (msg.result().channel() == null || !"trade".equalsIgnoreCase(msg.result().channel()))
            return Collections.emptyList();

        if (msg.result().data() == null || msg.result().data().isEmpty()) return Collections.emptyList();

        List<Trade> out = new ArrayList<>(msg.result().data().size());

        for (CryptoTrade t : msg.result().data()) {
            if (t == null) continue;
            if (t.i() == null || t.i().isBlank()) continue;
            if (t.p() == null || t.q() == null) continue;
            if (t.p().isBlank() || t.q().isBlank()) continue;

            out.add(new Trade(
                    Exchange.CRYPTOCOM,
                    t.i(),
                    Double.parseDouble(t.p()),
                    Double.parseDouble(t.q()),
                    t.s() == null ? null : t.s().toLowerCase(Locale.ROOT),
                    t.t()
            ));
        }

        return out.isEmpty() ? Collections.emptyList() : out;
    }

    // Gate parser
    record GateTrade(
            Long id,
            Long id_market,
            Long create_time,
            String create_time_ms,
            String side,
            String currency_pair,
            String amount,
            String price
    ) {}

    record GateMsg(
            Long time,
            String channel,
            String event,
            GateTrade result
    ) {}

    private List<Trade> parseGateTrades(String raw) {
        if (raw == null || raw.isBlank()) return Collections.emptyList();

        GateMsg msg = tryParse(raw, GateMsg.class);
        if (msg == null) return Collections.emptyList();

        if (msg.channel() == null || !"spot.trades".equalsIgnoreCase(msg.channel())) return Collections.emptyList();
        if (msg.event() == null || !"update".equalsIgnoreCase(msg.event())) return Collections.emptyList();

        GateTrade t = msg.result();
        if (t == null) return Collections.emptyList();

        if (t.currency_pair() == null || t.currency_pair().isBlank()) return Collections.emptyList();
        if (t.price() == null || t.price().isBlank()) return Collections.emptyList();
        if (t.amount() == null || t.amount().isBlank()) return Collections.emptyList();

        long tsMs;
        if (t.create_time_ms() != null && !t.create_time_ms().isBlank()) {
            String ms = t.create_time_ms().trim();
            int dot = ms.indexOf('.');
            if (dot >= 0) ms = ms.substring(0, dot);
            tsMs = Long.parseLong(ms);
        } else if (t.create_time() != null) {
            tsMs = t.create_time() * 1000L;
        } else {
            return Collections.emptyList();
        }

        String side = t.side() == null ? null : t.side().toLowerCase(Locale.ROOT);
        if (side != null && !side.equals("buy") && !side.equals("sell")) side = null;

        Trade trade = new Trade(
                Exchange.GATE,
                t.currency_pair(),
                Double.parseDouble(t.price()),
                Double.parseDouble(t.amount()),
                side,
                tsMs
        );

        return Collections.singletonList(trade);
    }

    // Upbit parser
    record UpbitTrade(
            String type,
            String code,
            Double trade_price,
            Double trade_volume,
            String ask_bid,
            Long trade_timestamp
    ) {}

    private List<Trade> parseUpbitTrades(String raw) {
        if (raw == null || raw.isBlank()) return Collections.emptyList();

        UpbitTrade t = tryParse(raw, UpbitTrade.class);
        if (t == null) return Collections.emptyList();
        if (!"trade".equalsIgnoreCase(t.type())) return Collections.emptyList();
        if (t.code() == null || t.trade_price() == null || t.trade_volume() == null || t.trade_timestamp() == null)
            return Collections.emptyList();

        Trade trade = new Trade(
                Exchange.UPBIT,
                t.code(),
                t.trade_price(),
                t.trade_volume(),
                normalizeUpbitSide(t.ask_bid()),
                t.trade_timestamp()
        );

        return Collections.singletonList(trade);
    }

    private String normalizeUpbitSide(String askBid) {
        if (askBid == null) return null;
        return switch (askBid.toUpperCase(Locale.ROOT)) {
            case "ASK" -> "sell";
            case "BID" -> "buy";
            default -> null;
        };
    }

    // OKX parser
    record OkxTrade(
            String instId,
            String px,
            String sz,
            String side,
            String ts
    ) {}

    record OkxArg(String channel, String instId) {}
    record OkxMsg(OkxArg arg, List<OkxTrade> data) {}

    private List<Trade> parseOkxTrades(String raw) {
        if (raw == null || raw.isBlank()) return Collections.emptyList();

        OkxMsg msg = tryParse(raw, OkxMsg.class);
        if (msg == null) return Collections.emptyList();

        if (msg.data() == null || msg.data().isEmpty()) return Collections.emptyList();
        if (msg.arg() == null || msg.arg().channel() == null) return Collections.emptyList();
        if (!"trades".equalsIgnoreCase(msg.arg().channel())) return Collections.emptyList();

        List<Trade> out = new ArrayList<>(msg.data().size());

        for (OkxTrade t : msg.data()) {
            if (t == null) continue;
            if (t.instId() == null || t.px() == null || t.sz() == null || t.ts() == null) continue;
            if (t.instId().isBlank() || t.px().isBlank() || t.sz().isBlank() || t.ts().isBlank()) continue;

            if (!t.instId().endsWith("-USDT")) continue;

            String side = t.side() == null ? null : t.side().toLowerCase(Locale.ROOT);
            if (side != null && !side.equals("buy") && !side.equals("sell")) side = null;

            out.add(new Trade(
                    Exchange.OKX,
                    t.instId(),
                    Double.parseDouble(t.px()),
                    Double.parseDouble(t.sz()),
                    side,
                    Long.parseLong(t.ts())
            ));
        }

        return out.isEmpty() ? Collections.emptyList() : out;
    }

    // MEXC parser
    record MexcDealPush(
            String symbol,          // ticker
            List<MexcDealItem> data,
            String channel,
            long ts
    ) {}

    record MexcDealItem(
            double p,   // price
            double v,   // volume
            int T,      // but/sell
            int O,
            int M,
            long t      // timestamp (ms)
    ) {}

    private List<Trade> parseMexcTrades(String raw) {
        if (raw == null || raw.isBlank()) return Collections.emptyList();

        MexcDealPush push;
        try {
            push = GSON.fromJson(raw, MexcDealPush.class);
        } catch (Exception e) {
            return Collections.emptyList();
        }

        if (push == null) return Collections.emptyList();
        if (push.channel() == null || !"push.deal".equalsIgnoreCase(push.channel()))
            return Collections.emptyList();
        if (push.symbol() == null || push.symbol().isBlank())
            return Collections.emptyList();
        if (push.data() == null || push.data().isEmpty())
            return Collections.emptyList();

        List<Trade> out = new ArrayList<>(push.data().size());

        for (MexcDealItem d : push.data()) {
            if (d == null) continue;

            String side = switch (d.T) {
                case 1 -> "buy";
                case 2 -> "sell";
                default -> null;
            };

            out.add(new Trade(
                    Exchange.MEXC,
                    push.symbol(),
                    d.p,
                    d.v,
                    side,
                    d.t
            ));
        }

        return out;
    }
}