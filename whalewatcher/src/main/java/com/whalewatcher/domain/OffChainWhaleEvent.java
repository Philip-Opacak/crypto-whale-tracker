package com.whalewatcher.domain;

/*
 * Represents a normalized whale trade event used throughout the WhaleWatcher system.
 *
 * This class standardizes trade data from various exchanges into a common structure
 * where all totals are expressed in USD.
*/

public record OffChainWhaleEvent(
        String id,
        Exchange exchange,
        String symbol,
        String side,
        double price,
        double quantity,
        double totalUsd,
        long timestampMs
) {}
