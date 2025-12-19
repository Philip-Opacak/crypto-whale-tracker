package com.whalewatcher.domain;

public record OnChainWhaleEvent(
        String id,
        Chain chain,
        String asset,          // "ETH"
        double amount,         // ETH amount
        String fromAddress,
        String toAddress,
        String txHash,
        long blockNumber,
        long timestampMs
) {}