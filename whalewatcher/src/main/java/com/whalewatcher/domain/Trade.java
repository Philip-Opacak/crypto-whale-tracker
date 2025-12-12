package com.whalewatcher.domain;

/*
 * Represents a single raw trade event received directly from an exchange's WebSocket feed.
 *
 * This object mirrors the data structure sent by the exchange before any processing or normalization takes place.
 */

public record Trade(
        Exchange exchange,
        String symbol,
        double price,
        double volume,
        String side,
        long timestamp
) {}
