package com.whalewatcher.domain;

/* Represents the trade direction (buy or sell) for a given transaction.
 *
 * This enum standardizes how trade sides are interpreted across different exchanges.
 * Each exchange may encode this information differently
*/

public enum Side {
    BUY, SELL;

    public static Side fromExchangeCode(String code) {
        return switch (code.toLowerCase()) {
            case "b", "buy" -> BUY;
            case "s", "sell" -> SELL;
            default -> throw new IllegalArgumentException("Unknown side: " + code);
        };
    }
}
