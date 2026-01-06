package com.whalewatcher.domain;

import java.math.BigDecimal;

public record OnChainWhaleEvent(
        Chain chain,
        Asset asset,
        BigDecimal amount,
        String fromAddress,
        String toAddress,
        String txHash,
        long blockNumber,
        long timestampMs
) {}