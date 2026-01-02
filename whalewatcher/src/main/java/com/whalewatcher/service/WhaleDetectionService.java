package com.whalewatcher.service;

import com.whalewatcher.domain.Asset;
import com.whalewatcher.domain.OffChainWhaleEvent;
import org.springframework.stereotype.Service;

@Service
public class WhaleDetectionService {

    public boolean isWhale(OffChainWhaleEvent event){
        Asset asset = event.asset();
        double usd = event.totalUsd();

        // Event thresholds based on official Whale Alert reporting limits
        // https://whale-alert.io/whales.html
        return switch (asset){
            case BTC, ETH , XRP ->  usd >= 50_000_000;
            case BNB, SOL ->  usd >= 20_000_000;
            default -> false;
        };
    }
}
