package com.whalewatcher;

import com.whalewatcher.domain.Asset;
import com.whalewatcher.domain.Exchange;
import com.whalewatcher.domain.Trade;
import com.whalewatcher.domain.OffChainWhaleEvent;
import com.whalewatcher.service.NormalizationService;
import com.whalewatcher.service.SymbolMapper;
import com.whalewatcher.service.WhaleDetectionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class NormalizationServiceTest {
    @Mock
    SymbolMapper symbolMapper;

    @Mock
    WhaleDetectionService whaleDetectionService;

    NormalizationService service;

    @BeforeEach
    void setup() {
        service = new NormalizationService(symbolMapper, whaleDetectionService);
    }

    @Test
    void normalizeAndFilterReturnsWhale() {
        Trade trade = new Trade(
                Exchange.KRAKEN,
                "XBT/USD",
                50000,
                2000,
                "b",
                1765602000000L
        );

        when(symbolMapper.normalize("XBT/USD", Exchange.KRAKEN))
                .thenReturn(Asset.BTC);

        when(whaleDetectionService.isWhale(any()))
                .thenReturn(true);

        Optional<OffChainWhaleEvent> result = service.normalizeAndFilter(trade);

        assertTrue(result.isPresent());
    }
}
