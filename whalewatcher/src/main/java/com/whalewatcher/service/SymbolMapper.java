package com.whalewatcher.service;

import com.whalewatcher.domain.Exchange;
import org.springframework.stereotype.Component;

import java.util.*;

/*Convert exchange-specific trading pairs into a consistent format, so the rest of the pipeline can
 *treat pairs the same regardless of where they came from.
 */

@Component
public class SymbolMapper {

    private static final Map<String, String> KRAKEN_MAP = Map.of(
            "XBT/USD", "BTC/USD",
            "BTC/USD", "BTC/USD",
            "ETH/USD", "ETH/USD",
            "BNB/USD", "BNB/USD",
            "SOL/USD", "SOL/USD",
            "XRP/USD", "XRP/USD"
    );

    private static final Map<String, String> BINANCE_MAP = Map.of(
            "BTCUSDT", "BTC/USD",
            "ETHUSDT", "ETH/USD",
            "BNBUSDT", "BNB/USD",
            "SOLUSDT", "SOL/USD",
            "XRPUSDT", "XRP/USD"
    );

    // Coinbase does not support BNB (coming soon)
    private static final Map<String, String> COINBASE_MAP = Map.of(
            "BTC-USD", "BTC/USD",
            "ETH-USD", "ETH/USD",
            "SOL-USD", "SOL/USD",
            "XRP-USD", "XRP/USD"
    );

    private final static Map<String, String> BYBIT_MAP = Map.of(
            "BTCUSDT", "BTC/USD",
            "ETHUSDT", "ETH/USD",
            "BNBUSDT", "BNB/USD",
            "SOLUSDT", "SOL/USD",
            "XRPUSDT", "XRP/USD"
    );

    // Upbit does not support BNB
    private final static Map<String, String> UPBIT_MAP = Map.of(
            "USDT-BTC", "BTC/USD",
            "USDT-ETH", "ETH/USD",
            "USDT-SOL", "SOL/USD",
            "USDT-XRP", "XRP/USD"
    );

    private final static Map<String, String> OKX_MAP = Map.of(
            "BTC-USDT", "BTC/USD",
            "ETH-USDT", "ETH/USD",
            "SOL-USDT", "SOL/USD",
            "XRP-USDT", "XRP/USD",
            "BNB-USDT", "BNB/USD"
    );

    private final static Map<String, String> BITGET_MAP = Map.of(
            "BTCUSDT", "BTC/USD",
            "ETHUSDT", "ETH/USD",
            "BNBUSDT", "BNB/USD",
            "SOLUSDT", "SOL/USD",
            "XRPUSDT", "XRP/USD"
    );

    private final static Map<String, String> GATE_MAP = Map.of(
            "BTC_USDT", "BTC/USD",
            "ETH_USDT", "ETH/USD",
            "BNB_USDT", "BNB/USD",
            "SOL_USDT", "SOL/USD",
            "XRP_USDT", "XRP/USD"
    );

    private final static Map<String, String> KUCOIN_MAP = Map.of(
            "BTC-USDT", "BTC/USD",
            "ETH-USDT", "ETH/USD",
            "SOL-USDT", "SOL/USD",
            "XRP-USDT", "XRP/USD",
            "BNB-USDT", "BNB/USD"
    );

    private final static Map<String, String> CRYPTOCOM_MAP = Map.of(
            "BTCUSD-PERP", "BTC/USD",
            "ETHUSD-PERP", "ETH/USD",
            "SOLUSD-PERP", "SOL/USD",
            "XRPUSD-PERP", "XRP/USD",
            "BNBUSD-PERP", "BNB/USD"
    );

    private final static Map<String, String> MEXC_MAP = Map.of(
            "BTC_USDT", "BTC/USD",
            "ETH_USDT", "ETH/USD",
            "BNB_USDT", "BNB/USD",
            "SOL_USDT", "SOL/USD",
            "XRP_USDT", "XRP/USD"
    );

    public String normalize(String rawSymbol, Exchange exchange) {
        if (rawSymbol == null || exchange == null) return null;

        String symbol = rawSymbol.trim().toUpperCase(Locale.ROOT);
        return switch (exchange) {
            case KRAKEN   -> KRAKEN_MAP.get(symbol);
            case BINANCE  -> BINANCE_MAP.get(symbol);
            case COINBASE -> COINBASE_MAP.get(symbol);
            case BYBIT -> BYBIT_MAP.get(symbol);
            case UPBIT -> UPBIT_MAP.get(symbol);
            case OKX -> OKX_MAP.get(symbol);
            case BITGET -> BITGET_MAP.get(symbol);
            case GATE -> GATE_MAP.get(symbol);
            case KUCOIN -> KUCOIN_MAP.get(symbol);
            case CRYPTOCOM -> CRYPTOCOM_MAP.get(symbol);
            case MEXC -> MEXC_MAP.get(symbol);
            default -> null;
        };
    }
}