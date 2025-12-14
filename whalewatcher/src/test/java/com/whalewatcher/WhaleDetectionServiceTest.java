package com.whalewatcher;

import com.whalewatcher.domain.Exchange;
import com.whalewatcher.domain.WhaleEvent;
import com.whalewatcher.service.WhaleDetectionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class WhaleDetectionServiceTest {
    private WhaleDetectionService service;

    @BeforeEach
    void setup() {
        service = new WhaleDetectionService();
    }

    @Test
    void btcWhaleAboveThreshold() {
        WhaleEvent event = new WhaleEvent(
                "id",
                Exchange.KRAKEN,
                "BTC/USD",
                "buy",
                50000,
                2000,
                100_000_000,
                1765602000000L
        );

        assertTrue(service.isWhale(event));
    }

    @Test
    void btcBelowThreshold() {
        WhaleEvent event = new WhaleEvent(
                "id",
                Exchange.KRAKEN,
                "BTC/USD",
                "buy",
                5,
                2,
                10,
                1765602000000L
        );

        assertFalse(service.isWhale(event));
    }
}
