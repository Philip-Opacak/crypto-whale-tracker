package com.whalewatcher.service;

import org.springframework.stereotype.Component;

@Component
public class SymbolMapper {

    public String normalize(String rawSymbol, String exchange) {
        if (exchange == null || rawSymbol == null) return rawSymbol;

        return switch (exchange.toLowerCase()) {
            case "kraken" -> rawSymbol.replace("XBT", "BTC");
            case "binance" -> formatBinanceSymbol(rawSymbol);
            case "coinbase" -> rawSymbol.replace("-", "/");
            default -> rawSymbol;
        };
    }

    private String formatBinanceSymbol(String symbol) {
        if (symbol.endsWith("USDT")) {
            return symbol.replace("USDT", "/USD");
        }
        return symbol;
    }
}