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

    private static final Map<String, String> COINBASE_MAP = Map.of(
            "BTC-USD", "BTC/USD",
            "ETH-USD", "ETH/USD",
            "BNB-USD", "BNB/USD",
            "SOL-USD", "SOL/USD",
            "XRP-USD", "XRP/USD"
    );

    public String normalize(String rawSymbol, Exchange exchange) {
        if (rawSymbol == null || exchange == null) return null;

        String symbol = rawSymbol.trim().toUpperCase(Locale.ROOT);
        return switch (exchange) {
            case KRAKEN   -> KRAKEN_MAP.get(symbol);
            case BINANCE  -> BINANCE_MAP.get(symbol);
            case COINBASE -> COINBASE_MAP.get(symbol);
            default -> null;
        };
    }
}