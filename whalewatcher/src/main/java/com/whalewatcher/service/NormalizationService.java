package com.whalewatcher.service;

import com.whalewatcher.domain.Side;
import com.whalewatcher.domain.Trade;
import com.whalewatcher.domain.OffChainWhaleEvent;
import org.springframework.stereotype.Service;

import java.util.Locale;
import java.util.Optional;
import java.util.UUID;

@Service
public class NormalizationService {

    private final SymbolMapper symbolMapper;
    private final WhaleDetectionService whaleDetectionService;

    public NormalizationService(SymbolMapper symbolMapper, WhaleDetectionService whaleDetectionService) {
        this.symbolMapper = symbolMapper;
        this.whaleDetectionService = whaleDetectionService;
    }

    public OffChainWhaleEvent normalize(Trade trade) {
        // 1) Symbol normalization
        String normalizedSymbol = symbolMapper.normalize(trade.symbol(), trade.exchange());

        if (normalizedSymbol == null) {
            return null;
        }

        // 2) Side normalization
        String side = Side.fromExchangeCode(trade.side()).name().toLowerCase(Locale.ROOT);

        double totalUsd = trade.price() * trade.volume();

        String id = UUID.randomUUID().toString();

        // 5) Build canonical event
        return new OffChainWhaleEvent(
                id,
                trade.exchange(),
                normalizedSymbol,
                side,
                trade.price(),
                trade.volume(),
                totalUsd,
                trade.timestamp()
        );
    }

    public Optional<OffChainWhaleEvent> normalizeAndFilter(Trade trade) {
        OffChainWhaleEvent event = normalize(trade);
        if (event == null) return Optional.empty();

        return whaleDetectionService.isWhale(event)
                ? Optional.of(event)
                : Optional.empty();
    }
}