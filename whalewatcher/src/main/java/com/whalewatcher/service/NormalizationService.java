package com.whalewatcher.service;

import com.whalewatcher.domain.Side;
import com.whalewatcher.domain.Trade;
import com.whalewatcher.domain.WhaleEvent;
import org.springframework.stereotype.Service;

import java.util.Locale;
import java.util.UUID;

@Service
public class NormalizationService {

    private final SymbolMapper symbolMapper;

    public NormalizationService(SymbolMapper symbolMapper) {
        this.symbolMapper = symbolMapper;
    }

    /** Main entry: Trade → WhaleEvent */
    public WhaleEvent normalize(Trade trade) {
        // 1) Symbol normalization
        String normalizedSymbol = symbolMapper.normalize(trade.symbol(), trade.exchange());
        String[] parts = normalizedSymbol.split("/");
        String base = parts.length > 0 ? parts[0] : trade.symbol();
        String quote = parts.length > 1 ? parts[1] : "USD"; // safe default

        // 2) Side normalization
        String side = Side.fromExchangeCode(trade.side()).name().toLowerCase(Locale.ROOT);

        // 3) Total in USD (pass-through for USD/USDT/USDC; add FX later if needed)
        double totalUsd = trade.price() * trade.volume();
        if (!isUsdLike(quote)) {
            // TODO: when you ingest non-USD quotes, multiply by an FX rate here.
            // totalUsd *= fxRates.get(quote);
        }

        // 4) Temporary unique id (can replace with deterministic id later)
        String id = UUID.randomUUID().toString();

        // 5) Build canonical event
        return new WhaleEvent(
                id,
                trade.exchange(),
                normalizedSymbol,
                base,
                quote,
                side,
                trade.price(),
                trade.volume(),
                totalUsd,
                trade.timestamp()
        );
    }

    private boolean isUsdLike(String quote) {
        String q = quote.toUpperCase(Locale.ROOT);
        return q.equals("USD") || q.equals("USDT") || q.equals("USDC");
    }
}