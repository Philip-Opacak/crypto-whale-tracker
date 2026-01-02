package com.whalewatcher.domain;

public record OnChainWhaleEvent(
        String id,
        Chain chain,
        Asset asset,
        double amount,
        String fromAddress,
        String toAddress,
        String txHash,
        long blockNumber,
        long timestampMs
) {}