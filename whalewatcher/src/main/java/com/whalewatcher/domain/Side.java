package com.whalewatcher.domain;

/* Represents the trade direction (buy or sell) for a given transaction.
 *
 * This enum standardizes how trade sides are interpreted across different exchanges.
 * Each exchange may encode this information differently
*/

import com.fasterxml.jackson.annotation.JsonCreator;

public enum Side {
    BUY, SELL;

    @JsonCreator
    public static Side fromExchangeCode(String code) {
        if (code == null) return null;
        return switch (code.toLowerCase()) {
            case "b", "buy" -> BUY;
            case "s", "sell" -> SELL;
            default -> throw new IllegalArgumentException("Unknown side: " + code);
        };
    }
}
