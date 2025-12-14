package com.whalewatcher;

import com.whalewatcher.domain.Exchange;
import com.whalewatcher.service.SymbolMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class SymbolMapperTest {

    private SymbolMapper mapper;

    @BeforeEach
    void setup() {
        mapper = new SymbolMapper();
    }

    @Test
    void krakenXbtIsNormalizedToBtc() {
        assertEquals("BTC/USD", mapper.normalize("XBT/USD", Exchange.KRAKEN));
    }

    @Test
    void BinanceBtcIsNormalizedToBTC(){
        assertEquals("BTC/USD", mapper.normalize("BTCUSDT", Exchange.BINANCE));
    }

    @Test
    void CoinBaseBtcIsNormalizedToBTC(){
        assertEquals("BTC/USD", mapper.normalize("BTC-USD", Exchange.COINBASE));
    }

    @Test
    void KrakenEthIsAlreadyNormalizedToEth(){
        assertEquals("ETH/USD", mapper.normalize("ETH/USD", Exchange.KRAKEN));
    }

    @Test
    void KrakenUnknownSymbolToNull(){
        assertNull(mapper.normalize("DOGE/USD", Exchange.KRAKEN));
    }

    @Test
    void BinanceUnknownSymbolToNull(){
        assertNull(mapper.normalize("DOGE/USD", Exchange.BINANCE));
    }

    @Test
    void CoinBaseUnknownSymbolToNull(){
        assertNull(mapper.normalize("DOGE/USD", Exchange.COINBASE));
    }

    @Test
    void KrakenLowercaseSymbol(){
        assertEquals("BTC/USD", mapper.normalize("xbt/usd", Exchange.KRAKEN));
    }
}
