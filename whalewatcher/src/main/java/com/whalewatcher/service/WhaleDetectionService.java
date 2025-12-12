package com.whalewatcher.service;

import com.whalewatcher.domain.WhaleEvent;
import org.springframework.stereotype.Service;

@Service
public class WhaleDetectionService {

    public boolean isWhale(WhaleEvent event){
        String symbol = event.symbol();
        double usd = event.totalUsd();

        return switch (symbol){
            case "BTC/USD", "ETH/USD" , "XRP/USD" ->  usd >= 50_000_000;
            case "BNB/USD", "SOL/USD" ->  usd >= 20_000_000;
            default -> false;
        };
    }
}
