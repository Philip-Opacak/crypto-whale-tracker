package com.whalewatcher.service;

import com.whalewatcher.domain.Asset;
import com.whalewatcher.domain.Exchange;
import org.springframework.stereotype.Component;

import java.util.*;

/*Convert exchange-specific trading pairs into a consistent format, so the rest of the pipeline can
 *assets pairs the same regardless of where they came from.
 */

@Component
public class SymbolMapper {

    private static final Map<String, Asset> KRAKEN_MAP = Map.of(
            "XBT/USD", Asset.BTC,
            "BTC/USD", Asset.BTC,
            "ETH/USD", Asset.ETH,
            "BNB/USD", Asset.BNB,
            "SOL/USD", Asset.SOL,
            "XRP/USD", Asset.XRP
    );

    private static final Map<String, Asset> BINANCE_MAP = Map.of(
            "BTCUSDT", Asset.BTC,
            "ETHUSDT", Asset.ETH,
            "BNBUSDT", Asset.BNB,
            "SOLUSDT", Asset.SOL,
            "XRPUSDT", Asset.XRP
    );

    // Coinbase does not support BNB
    private static final Map<String, Asset> COINBASE_MAP = Map.of(
            "BTC-USD", Asset.BTC,
            "ETH-USD", Asset.ETH,
            "SOL-USD", Asset.SOL,
            "XRP-USD", Asset.XRP
    );

    private static final Map<String, Asset> BYBIT_MAP = Map.of(
            "BTCUSDT", Asset.BTC,
            "ETHUSDT", Asset.ETH,
            "BNBUSDT", Asset.BNB,
            "SOLUSDT", Asset.SOL,
            "XRPUSDT", Asset.XRP
    );

    // Upbit does not support BNB
    private static final Map<String, Asset> UPBIT_MAP = Map.of(
            "USDT-BTC", Asset.BTC,
            "USDT-ETH", Asset.ETH,
            "USDT-SOL", Asset.SOL,
            "USDT-XRP", Asset.XRP
    );

    private static final Map<String, Asset> OKX_MAP = Map.of(
            "BTC-USDT", Asset.BTC,
            "ETH-USDT", Asset.ETH,
            "SOL-USDT", Asset.SOL,
            "XRP-USDT", Asset.XRP,
            "BNB-USDT", Asset.BNB
    );

    private static final Map<String, Asset> BITGET_MAP = Map.of(
            "BTCUSDT", Asset.BTC,
            "ETHUSDT", Asset.ETH,
            "BNBUSDT", Asset.BNB,
            "SOLUSDT", Asset.SOL,
            "XRPUSDT", Asset.XRP
    );

    private static final Map<String, Asset> GATE_MAP = Map.of(
            "BTC_USDT", Asset.BTC,
            "ETH_USDT", Asset.ETH,
            "BNB_USDT", Asset.BNB,
            "SOL_USDT", Asset.SOL,
            "XRP_USDT", Asset.XRP
    );

    private static final Map<String, Asset> KUCOIN_MAP = Map.of(
            "BTC-USDT", Asset.BTC,
            "ETH-USDT", Asset.ETH,
            "BNB-USDT", Asset.BNB,
            "SOL-USDT", Asset.SOL,
            "XRP-USDT", Asset.XRP
    );

    private static final Map<String, Asset> CRYPTOCOM_MAP = Map.of(
            "BTCUSD-PERP", Asset.BTC,
            "ETHUSD-PERP", Asset.ETH,
            "SOLUSD-PERP", Asset.SOL,
            "XRPUSD-PERP", Asset.XRP,
            "BNBUSD-PERP", Asset.BNB
    );

    private static final Map<String, Asset> MEXC_MAP = Map.of(
            "BTC_USDT", Asset.BTC,
            "ETH_USDT", Asset.ETH,
            "BNB_USDT", Asset.BNB,
            "SOL_USDT", Asset.SOL,
            "XRP_USDT", Asset.XRP
    );

    public Asset normalize(String rawSymbol, Exchange exchange) {
        if (rawSymbol == null || exchange == null) return null;

        String symbol = rawSymbol.trim().toUpperCase(Locale.ROOT);

        return switch (exchange) {
            case KRAKEN     -> KRAKEN_MAP.get(symbol);
            case BINANCE    -> BINANCE_MAP.get(symbol);
            case COINBASE   -> COINBASE_MAP.get(symbol);
            case BYBIT      -> BYBIT_MAP.get(symbol);
            case UPBIT      -> UPBIT_MAP.get(symbol);
            case OKX        -> OKX_MAP.get(symbol);
            case BITGET     -> BITGET_MAP.get(symbol);
            case GATE       -> GATE_MAP.get(symbol);
            case KUCOIN     -> KUCOIN_MAP.get(symbol);
            case CRYPTOCOM  -> CRYPTOCOM_MAP.get(symbol);
            case MEXC       -> MEXC_MAP.get(symbol);
            default         -> null;
        };
    }
}