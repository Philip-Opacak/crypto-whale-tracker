package com.whalewatcher;

import com.whalewatcher.domain.Exchange;
import com.whalewatcher.domain.Trade;
import com.whalewatcher.ingest.offchain.websocket.RawWsBus;
import com.whalewatcher.ingest.offchain.websocket.WsMessageParser;
import com.whalewatcher.ingest.offchain.websocket.WsWorkers;
import com.whalewatcher.service.IngestionService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.Mockito.*;

/**
 * Tests for WsWorkers focusing on message flow, error handling,
 * and shutdown of worker threads.
 */

class WsWorkersTest {

    private RawWsBus bus;
    private IngestionService ingestionService;
    private WsMessageParser parser;
    private WsWorkers workers;

    @BeforeEach
    void setup(){
        ingestionService = mock(IngestionService.class);
        parser = mock(WsMessageParser.class);
        bus = new RawWsBus();
        workers = new WsWorkers(bus, ingestionService, parser);
        workers.start();
    }

    @AfterEach
    void tearDown(){
        workers.stop();
    }

    @Test
    void worker_ingestsTradesProducedByParser(){
        String raw = "{\"some\":\"json\"}";
        Exchange ex = Exchange.BINANCE;

        Trade trade1 = new Trade(ex, "BTCUSDT", 42000.5, 0.01, "buy", 1700000000123L);
        Trade trade2 = new Trade(ex, "BTCUSDT", 42001.0, 0.02, "sell", 1700000000456L);

        when(parser.parse(ex, raw)).thenReturn(List.of(trade1, trade2));

        // public list of trades to bus
        bus.publish(ex, raw);

        // ensure the ingest of both trades was called within 500ms
        verify(ingestionService, timeout(500)).ingest(trade1);
        verify(ingestionService, timeout(500)).ingest(trade2);
        verifyNoMoreInteractions(ingestionService);
    }

    @Test
    void worker_doesNothingWhenParserReturnsEmptyList(){
        String raw = "{\"some\":\"json\"}";
        Exchange ex = Exchange.BINANCE;

        when(parser.parse(ex, raw)).thenReturn(List.of());

        bus.publish(ex, raw);

        // confirms message was processed
        verify(parser, timeout(500)).parse(ex, raw);
        // If no Trade output from the parser then there should be in calls to the ingestion service
        verifyNoInteractions(ingestionService);
    }


    @Test
    void worker_continuesRunningWhenParserThrowsException(){
        Exchange ex = Exchange.BINANCE;

        String rawBad = "{\"bad\":json}";
        String rawGood = "{\"good\":json}";

        Trade trade = new Trade(ex, "BTCUSDT", 42000.5, 0.01, "buy", 1700000000123L);

        // First call throws an exception
        when(parser.parse(ex, rawBad))
                .thenThrow(new RuntimeException("mock failure"));
        // second call will succeed
        when(parser.parse(ex, rawGood))
                .thenReturn(List.of(trade));

        bus.publish(ex, rawBad);
        bus.publish(ex, rawGood);

        // ensure both trades were called on by the parser
        verify(parser, timeout(500)).parse(ex, rawBad);
        verify(parser, timeout(500)).parse(ex, rawGood);

        // after the exception, worker still ingests the next message
        verify(ingestionService, timeout(500)).ingest(trade);
    }

    @Test
    void worker_stopsGracefullyWhenStopped(){
        Exchange ex = Exchange.BINANCE;

        String raw1 = "{\"msg\":1}";
        String raw2 = "{\"msg\":2}";

        Trade trade1 = new Trade(ex, "BTCUSDT", 42000.5, 0.01, "buy", 1700000000123L);
        Trade trade2 = new Trade(ex, "BTCUSDT", 42001.0, 0.02, "sell", 1700000000456L);

        when(parser.parse(ex, raw1)).thenReturn(List.of(trade1));
        when(parser.parse(ex, raw2)).thenReturn(List.of(trade2));

        bus.publish(ex, raw1);
        verify(ingestionService, timeout(500)).ingest(trade1);

        // stop workers
        workers.stop();

        // publish after stop
        bus.publish(ex, raw2);

        // trade2 should NOT be ingested after stop
        verify(ingestionService, after(200).never()).ingest(trade2);
    }
}