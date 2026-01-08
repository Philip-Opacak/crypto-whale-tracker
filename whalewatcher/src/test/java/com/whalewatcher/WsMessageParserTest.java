package com.whalewatcher;

import com.whalewatcher.domain.Exchange;
import com.whalewatcher.domain.Trade;
import com.whalewatcher.ingest.offchain.websocket.WsMessageParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Parser tests for WsMessageParser
 *
 * These tests verify that WebSocket messages from each supported exchange
 * are correctly parsed into Trade objects using representative
 * example payloads based on the exchanges’ documented message formats.
 *
 * For each exchange, the tests cover:
 *  - A valid trade message to ensure correct field mapping
 *  - A non-trade or irrelevant message to ensure the parser safely ignores
 *    incoming messages that should not produce trades.
 */

class WsMessageParserTest {

    WsMessageParser parser;

    @BeforeEach
    void setup(){
        parser = new WsMessageParser();
    }

    // Check for invalid json on all exchanges

    @Test
    void parse_invalidJson_returnsEmptyList_forAllExchanges(){
        String invalidJson = "{ not valid json";

        for (Exchange exchange : Exchange.values()) {
            List<Trade> out = parser.parse(exchange, invalidJson);

            assertNotNull(out, exchange + " returned null");
            assertTrue(out.isEmpty(), exchange + " did not return empty list");
        }
    }

    // BINANCE

    @Test
    void binance_tradeMessage_parsesOneTrade(){
        String raw = """
        {
            "stream":"btcusdt@trade",
            "data":{
            "e":"trade",
            "E":1700000000000,
            "s":"BTCUSDT",
            "p":"42000.5",
            "q":"0.01",
            "T":1700000000123,
            "m":false
            }
        }
        """;

        List<Trade> out = parser.parse(Exchange.BINANCE, raw);

        assertEquals(1, out.size());

        Trade t = out.get(0);
        assertEquals(Exchange.BINANCE, t.exchange());
        assertEquals("BTCUSDT", t.symbol());
        assertEquals(42000.5, t.price(), 1e-9);
        assertEquals(0.01, t.volume(), 1e-12);
        assertEquals("buy", t.side());
        assertEquals(1700000000123L, t.timestamp());
    }

    @Test
    void binance_nonTradeMessage_isIgnored(){
        String raw = """
        {
            "stream":"btcusdt@bookTicker",
            "data":{
            "e":"bookTicker",
            "s":"BTCUSDT",
            "p":"42000.5",
            "q":"0.01",
            "T":1700000000123,
            "m":false
            }
        }
        """;

        List<Trade> out = parser.parse(Exchange.BINANCE, raw);

        assertNotNull(out);
        assertTrue(out.isEmpty());
    }

    // COINBASE

    @Test
    void coinbase_marketTrades_parsesTrades(){
        String raw = """
        {
            "channel":"market_trades",
            "timestamp":"2024-01-01T00:00:00Z",
            "events":[
            {
                "type":"update",
                "trades":[
                {
                    "trade_id":"1",
                    "product_id":"BTC-USD",
                    "price":"42000.5",
                    "size":"0.01",
                    "side":"BUY",
                    "time":"2024-01-01T00:00:01Z"
                },
                {
                    "trade_id":"2",
                    "product_id":"BTC-USD",
                    "price":"42001.0",
                    "size":"0.02",
                    "side":"SELL",
                    "time":"2024-01-01T00:00:02Z"
                }
            ]
            }
        ]
        }
        """;

        List<Trade> out = parser.parse(Exchange.COINBASE, raw);

        assertEquals(2, out.size());

        Trade t1 = out.get(0);
        assertEquals(Exchange.COINBASE, t1.exchange());
        assertEquals("BTC-USD", t1.symbol());
        assertEquals(42000.5, t1.price(), 1e-9);
        assertEquals(0.01, t1.volume(), 1e-12);
        assertEquals("buy", t1.side());
        assertEquals(Instant.parse("2024-01-01T00:00:01Z").toEpochMilli(), t1.timestamp());

        Trade t2 = out.get(1);
        assertEquals(Exchange.COINBASE, t2.exchange());
        assertEquals("BTC-USD", t2.symbol());
        assertEquals(42001.0, t2.price(), 1e-9);
        assertEquals(0.02, t2.volume(), 1e-12);
        assertEquals("sell", t2.side());
        assertEquals(Instant.parse("2024-01-01T00:00:02Z").toEpochMilli(), t2.timestamp());
    }

    @Test
    void coinbase_wrongChannel_isIgnored(){
        String raw = """
        {
            "channel":"heartbeats",
            "timestamp":"2024-01-01T00:00:00Z"
        }
        """;

        List<Trade> out = parser.parse(Exchange.COINBASE, raw);

        assertNotNull(out);
        assertTrue(out.isEmpty());
    }

    // KUCOIN

    @Test
    void kucoin_tradeMessage_parsesOneTrade(){
        String raw = """
        {
            "type":"message",
            "subject":"trade.l3match",
            "data":{
                "price":"42000.5",
                "side":"BUY",
                "size":"0.01",
                "symbol":"BTC-USDT",
                "time":"1700000000123000000"
            }
        }
        """;

        List<Trade> out = parser.parse(Exchange.KUCOIN, raw);

        assertEquals(1, out.size());

        Trade t = out.get(0);
        assertEquals(Exchange.KUCOIN, t.exchange());
        assertEquals("BTC-USDT", t.symbol());
        assertEquals(42000.5, t.price(), 1e-9);
        assertEquals(0.01, t.volume(), 1e-12);
        assertEquals("buy", t.side());

        // KuCoin sends nanoseconds, the parser will convert to milliseconds
        assertEquals(1700000000123L, t.timestamp());
    }

    @Test
    void kucoin_wrongTypeOrSubject_isIgnored(){
        String raw = """
            {
            "type":"message",
            "subject":"trade.match",
            "data":{
                "price":"42000.5",
                "side":"BUY",
                "size":"0.01",
                "symbol":"BTC-USDT",
                "time":"1700000000123000000"
            }
        }
        """;

        List<Trade> out = parser.parse(Exchange.KUCOIN, raw);

        assertNotNull(out);
        assertTrue(out.isEmpty());
    }

    // KRAKEN

    @Test
    void kraken_tradeChannel_parsesTrades(){
        String raw = """
        {
            "channel":"trade",
            "type":"snapshot",
            "data":[
                {
                "symbol":"BTC/USD",
                "side":"buy",
                "price":42000.5,
                "qty":0.01,
                "timestamp":"2024-01-01T00:00:01Z"
                },
                {
                "symbol":"BTC/USD",
                "side":"sell",
                "price":42001.0,
                "qty":0.02,
                "timestamp":"2024-01-01T00:00:02Z"
                }
            ]
        }
        """;

        List<Trade> out = parser.parse(Exchange.KRAKEN, raw);

        assertEquals(2, out.size());

        Trade t1 = out.get(0);
        assertEquals(Exchange.KRAKEN, t1.exchange());
        assertEquals("BTC/USD", t1.symbol());
        assertEquals(42000.5, t1.price(), 1e-9);
        assertEquals(0.01, t1.volume(), 1e-12);
        assertEquals("buy", t1.side());
        assertEquals(Instant.parse("2024-01-01T00:00:01Z").toEpochMilli(), t1.timestamp());

        Trade t2 = out.get(1);
        assertEquals(Exchange.KRAKEN, t2.exchange());
        assertEquals("BTC/USD", t2.symbol());
        assertEquals(42001.0, t2.price(), 1e-9);
        assertEquals(0.02, t2.volume(), 1e-12);
        assertEquals("sell", t2.side());
        assertEquals(Instant.parse("2024-01-01T00:00:02Z").toEpochMilli(), t2.timestamp());
    }

    @Test
    void kraken_nonTradeChannel_isIgnored(){
        String raw = """
        {
            "channel":"ticker",
            "type":"snapshot",
            "data":[
                {
                "symbol":"BTC/USD",
                "side":"buy",
                "price":42000.5,
                "qty":0.01,
                "timestamp":"2024-01-01T00:00:01Z"
                }
            ]
        }
        """;

        List<Trade> out = parser.parse(Exchange.KRAKEN, raw);

        assertNotNull(out);
        assertTrue(out.isEmpty());
    }

    // BITGET

    @Test
    void bitget_tradeMessage_parsesTrades(){
        String raw = """
        {
            "action":"update",
            "arg":{
                "instType":"SPOT",
                "channel":"trade",
                "instId":"BTCUSDT"
            },
            "data":[
            {
                "ts":"1700000000123",
                "price":"42000.5",
                "size":"0.01",
                "side":"buy",
                "tradeId":"1"
            },
            {
                "ts":"1700000000456",
                "price":"42001.0",
                "size":"0.02",
                "side":"sell",
                "tradeId":"2"
            }
        ],
        "ts":1700000000000
        }
        """;

        List<Trade> out = parser.parse(Exchange.BITGET, raw);

        assertEquals(2, out.size());

        Trade t1 = out.get(0);
        assertEquals(Exchange.BITGET, t1.exchange());
        assertEquals("BTCUSDT", t1.symbol()); // comes from arg.instId
        assertEquals(42000.5, t1.price(), 1e-9);
        assertEquals(0.01, t1.volume(), 1e-12);
        assertEquals("buy", t1.side());
        assertEquals(1700000000123L, t1.timestamp());

        Trade t2 = out.get(1);
        assertEquals(Exchange.BITGET, t2.exchange());
        assertEquals("BTCUSDT", t2.symbol());
        assertEquals(42001.0, t2.price(), 1e-9);
        assertEquals(0.02, t2.volume(), 1e-12);
        assertEquals("sell", t2.side());
        assertEquals(1700000000456L, t2.timestamp());
    }

    @Test
    void bitget_pongOrNonSpot_isIgnored(){
        String raw = "pong";

        List<Trade> out = parser.parse(Exchange.BITGET, raw);

        assertNotNull(out);
        assertTrue(out.isEmpty());
    }

    // BYBIT

    @Test
    void bybit_tradeTopic_parsesTrades(){
        String raw = """
        {
            "topic":"publicTrade.BTCUSDT",
            "data":[
                { "p":"42000.5", "v":"0.01", "S":"Buy",  "T":1700000000123 },
                { "p":"42001.0", "v":"0.02", "S":"Sell", "T":1700000000456 }
            ]
        }
        """;

        List<Trade> out = parser.parse(Exchange.BYBIT, raw);

        assertEquals(2, out.size());

        Trade t1 = out.get(0);
        assertEquals(Exchange.BYBIT, t1.exchange());
        assertEquals("BTCUSDT", t1.symbol());       // extracted from topic
        assertEquals(42000.5, t1.price(), 1e-9);
        assertEquals(0.01, t1.volume(), 1e-12);
        assertEquals("buy", t1.side());
        assertEquals(1700000000123L, t1.timestamp());

        Trade t2 = out.get(1);
        assertEquals(Exchange.BYBIT, t2.exchange());
        assertEquals("BTCUSDT", t2.symbol());
        assertEquals(42001.0, t2.price(), 1e-9);
        assertEquals(0.02, t2.volume(), 1e-12);
        assertEquals("sell", t2.side());
        assertEquals(1700000000456L, t2.timestamp());
    }

    @Test
    void bybit_invalidTopicOrEmptyData_isIgnored(){
        String raw = """
        {
            "topic":"publicTrade.BTCUSDT",
            "data":[]
        }
        """;

        List<Trade> out = parser.parse(Exchange.BYBIT, raw);

        assertNotNull(out);
        assertTrue(out.isEmpty());
    }

    // CRYPTO.COM

    @Test
    void cryptocom_tradeSubscription_parsesTrades(){
        String raw = """
        {
            "id": 1,
            "method": "subscribe",
            "code": 0,
            "result": {
                "channel": "trade",
                "instrument_name": "BTC_USDT",
                "subscription": "trade.BTC_USDT",
                "data": [
                { "d":"1", "t":1700000000123, "p":"42000.5", "q":"0.01", "s":"BUY",  "i":"BTC_USDT" },
                { "d":"2", "t":1700000000456, "p":"42001.0", "q":"0.02", "s":"SELL", "i":"BTC_USDT" }
                ]
            }
        }
        """;

        List<Trade> out = parser.parse(Exchange.CRYPTOCOM, raw);

        assertEquals(2, out.size());

        Trade t1 = out.get(0);
        assertEquals(Exchange.CRYPTOCOM, t1.exchange());
        assertEquals("BTC_USDT", t1.symbol());
        assertEquals(42000.5, t1.price(), 1e-9);
        assertEquals(0.01, t1.volume(), 1e-12);
        assertEquals("buy", t1.side());
        assertEquals(1700000000123L, t1.timestamp());

        Trade t2 = out.get(1);
        assertEquals(Exchange.CRYPTOCOM, t2.exchange());
        assertEquals("BTC_USDT", t2.symbol());
        assertEquals(42001.0, t2.price(), 1e-9);
        assertEquals(0.02, t2.volume(), 1e-12);
        assertEquals("sell", t2.side());
        assertEquals(1700000000456L, t2.timestamp());
    }

    @Test
    void cryptocom_nonTradeOrError_isIgnored(){
        String raw = """
        {
            "id": 1,
            "method": "subscribe",
            "code": 10001,
            "result": {
                "channel": "trade",
                "instrument_name": "BTC_USDT",
                "subscription": "trade.BTC_USDT",
                "data": [
                { "d":"1", "t":1700000000123, "p":"42000.5", "q":"0.01", "s":"BUY", "i":"BTC_USDT" }
                ]
            }
        }
        """;

        List<Trade> out = parser.parse(Exchange.CRYPTOCOM, raw);

        assertNotNull(out);
        assertTrue(out.isEmpty());
    }

    // GATE

    @Test
    void gate_spotTradeUpdate_parsesTrade(){
        String raw = """
        {
            "time": 1700000000,
            "channel": "spot.trades",
            "event": "update",
            "result": {
                "id": 123,
                "id_market": 0,
                "create_time": 1700000000,
                "create_time_ms": "1700000000123.0",
                "side": "buy",
                "currency_pair": "BTC_USDT",
                "amount": "0.01",
                "price": "42000.5"
            }
        }
        """;

        List<Trade> out = parser.parse(Exchange.GATE, raw);

        assertEquals(1, out.size());

        Trade t = out.get(0);
        assertEquals(Exchange.GATE, t.exchange());
        assertEquals("BTC_USDT", t.symbol());
        assertEquals(42000.5, t.price(), 1e-9);
        assertEquals(0.01, t.volume(), 1e-12);
        assertEquals("buy", t.side());
        assertEquals(1700000000123L, t.timestamp());
    }

    @Test
    void gate_wrongChannelOrEvent_isIgnored(){
        String raw = """
        {
            "time": 1700000000,
            "channel": "spot.trades",
            "event": "subscribe",
            "result": {
                "create_time_ms": "1700000000123.0",
                "side": "buy",
                "currency_pair": "BTC_USDT",
                "amount": "0.01",
                "price": "42000.5"
            }
        }
        """;

        List<Trade> out = parser.parse(Exchange.GATE, raw);

        assertNotNull(out);
        assertTrue(out.isEmpty());
    }

    // UPBIT

    @Test
    void upbit_tradeType_parsesTrade(){
        String raw = """
        {
            "time": 1700000000,
            "channel": "spot.trades",
            "event": "update",
            "result": {
                "id": 123,
                "id_market": 0,
                "create_time": 1700000000,
                "create_time_ms": "1700000000123.0",
                "side": "buy",
                "currency_pair": "BTC_USDT",
                "amount": "0.01",
                "price": "42000.5"
            }
        }
        """;

        List<Trade> out = parser.parse(Exchange.GATE, raw);

        assertEquals(1, out.size());

        Trade t = out.get(0);
        assertEquals(Exchange.GATE, t.exchange());
        assertEquals("BTC_USDT", t.symbol());
        assertEquals(42000.5, t.price(), 1e-9);
        assertEquals(0.01, t.volume(), 1e-12);
        assertEquals("buy", t.side());
        assertEquals(1700000000123L, t.timestamp());

    }

    @Test
    void upbit_nonTradeType_isIgnored(){
        String raw = """
        {
            "type":"orderbook",
            "code":"KRW-BTC",
            "trade_price":42000.5,
            "trade_volume":0.01,
            "ask_bid":"BID",
            "trade_timestamp":1700000000123
        }
        """;

        List<Trade> out = parser.parse(Exchange.UPBIT, raw);

        assertNotNull(out);
        assertTrue(out.isEmpty());
    }

    // OKX

    @Test
    void okx_tradesChannel_parsesTrades(){
        String raw = """
        {
            "arg": { "channel":"trades", "instId":"BTC-USDT" },
            "data": [
                { "instId":"BTC-USDT", "px":"42000.5", "sz":"0.01", "side":"buy",  "ts":"1700000000123" },
                { "instId":"BTC-USDT", "px":"42001.0", "sz":"0.02", "side":"sell", "ts":"1700000000456" }
            ]
        }
        """;

        List<Trade> out = parser.parse(Exchange.OKX, raw);

        assertEquals(2, out.size());

        Trade t1 = out.get(0);
        assertEquals(Exchange.OKX, t1.exchange());
        assertEquals("BTC-USDT", t1.symbol());
        assertEquals(42000.5, t1.price(), 1e-9);
        assertEquals(0.01, t1.volume(), 1e-12);
        assertEquals("buy", t1.side());
        assertEquals(1700000000123L, t1.timestamp());

        Trade t2 = out.get(1);
        assertEquals(Exchange.OKX, t2.exchange());
        assertEquals("BTC-USDT", t2.symbol());
        assertEquals(42001.0, t2.price(), 1e-9);
        assertEquals(0.02, t2.volume(), 1e-12);
        assertEquals("sell", t2.side());
        assertEquals(1700000000456L, t2.timestamp());
    }

    @Test
    void okx_nonUsdtOrWrongChannel_isIgnored(){
        String raw = """
        {
            "arg": { "channel":"trades", "instId":"BTC-USD" },
            "data": [
                { "instId":"BTC-USD", "px":"42000.5", "sz":"0.01", "side":"buy", "ts":"1700000000123" }
            ]
        }
        """;

        List<Trade> out = parser.parse(Exchange.OKX, raw);

        assertNotNull(out);
        assertTrue(out.isEmpty());
    }

    // MEXC

    @Test
    void mexc_pushDeal_parsesTrades(){
        String raw = """
        {
            "symbol":"BTC_USDT",
            "channel":"push.deal",
            "ts":1700000000000,
            "data":[
                { "p":42000.5, "v":0.01, "T":1, "O":0, "M":0, "t":1700000000123 },
                { "p":42001.0, "v":0.02, "T":2, "O":0, "M":0, "t":1700000000456 }
            ]
        }
        """;

        List<Trade> out = parser.parse(Exchange.MEXC, raw);

        assertEquals(2, out.size());

        Trade t1 = out.get(0);
        assertEquals(Exchange.MEXC, t1.exchange());
        assertEquals("BTC_USDT", t1.symbol());
        assertEquals(42000.5, t1.price(), 1e-9);
        assertEquals(0.01, t1.volume(), 1e-12);
        assertEquals("buy", t1.side());
        assertEquals(1700000000123L, t1.timestamp());

        Trade t2 = out.get(1);
        assertEquals(Exchange.MEXC, t2.exchange());
        assertEquals("BTC_USDT", t2.symbol());
        assertEquals(42001.0, t2.price(), 1e-9);
        assertEquals(0.02, t2.volume(), 1e-12);
        assertEquals("sell", t2.side());
        assertEquals(1700000000456L, t2.timestamp());
    }

    @Test
    void mexc_wrongChannelOrEmptyData_isIgnored(){
        String raw = """
        {
            "symbol":"BTC_USDT",
            "channel":"push.ticker",
            "ts":1700000000000,
            "data":[
                { "p":42000.5, "v":0.01, "T":1, "O":0, "M":0, "t":1700000000123 }
            ]
        }
        """;

        List<Trade> out = parser.parse(Exchange.MEXC, raw);

        assertNotNull(out);
        assertTrue(out.isEmpty());
    }
}