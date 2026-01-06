package com.whalewatcher;

import com.whalewatcher.controller.OffChainController;
import com.whalewatcher.domain.Asset;
import com.whalewatcher.domain.Exchange;
import com.whalewatcher.domain.OffChainWhaleEvent;
import com.whalewatcher.repository.WhaleEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

@WebMvcTest(OffChainController.class)
public class OffChainControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private WhaleEventRepository whaleEventRepository;

    private OffChainWhaleEvent btcBinanceBuy;
    private OffChainWhaleEvent ethKrakenSell;

    @BeforeEach
    void setupData() {
        btcBinanceBuy = new OffChainWhaleEvent(
                "1",
                Exchange.BINANCE,
                Asset.BTC,
                "BUY",
                20_000_000,
                2,
                40_000_000,
                17660000
                );

        ethKrakenSell = new OffChainWhaleEvent(
                "2",
                Exchange.KRAKEN,
                Asset.ETH,
                "SELL",
                30_000_000,
                2,
                60_000_000,
                17660000
        );
    }

    // /offchain/whales
    @Test
    void getAllWhales_returns200AndList() throws Exception {
        when(whaleEventRepository.getAll(2)).thenReturn(List.of(btcBinanceBuy, ethKrakenSell));

        mockMvc.perform(get("/offchain/whales").param("limit", "2"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2))

                .andExpect(jsonPath("$[0].id").value("1"))
                .andExpect(jsonPath("$[0].exchange").value("BINANCE"))
                .andExpect(jsonPath("$[0].asset").value("BTC"))
                .andExpect(jsonPath("$[0].side").value("BUY"))
                .andExpect(jsonPath("$[0].price").value(20_000_000))
                .andExpect(jsonPath("$[0].quantity").value(2))
                .andExpect(jsonPath("$[0].totalUsd").value(40_000_000))
                .andExpect(jsonPath("$[0].timestampMs").value(17660000))

                .andExpect(jsonPath("$[1].id").value("2"))
                .andExpect(jsonPath("$[1].exchange").value("KRAKEN"))
                .andExpect(jsonPath("$[1].asset").value("ETH"))
                .andExpect(jsonPath("$[1].side").value("SELL"));

        verify(whaleEventRepository).getAll(2);
        verifyNoMoreInteractions(whaleEventRepository);
    }

    @Test
    void getAllWhales_usesDefaultLimitWhenNotProvided() throws Exception{
        when(whaleEventRepository.getAll(100)).thenReturn(List.of(btcBinanceBuy, ethKrakenSell));

        mockMvc.perform(get("/offchain/whales"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2))

                .andExpect(jsonPath("$[0].id").value("1"))
                .andExpect(jsonPath("$[0].exchange").value("BINANCE"))
                .andExpect(jsonPath("$[0].asset").value("BTC"))
                .andExpect(jsonPath("$[0].side").value("BUY"))
                .andExpect(jsonPath("$[0].price").value(20_000_000))
                .andExpect(jsonPath("$[0].quantity").value(2))
                .andExpect(jsonPath("$[0].totalUsd").value(40_000_000))
                .andExpect(jsonPath("$[0].timestampMs").value(17660000))

                .andExpect(jsonPath("$[1].id").value("2"))
                .andExpect(jsonPath("$[1].exchange").value("KRAKEN"))
                .andExpect(jsonPath("$[1].asset").value("ETH"))
                .andExpect(jsonPath("$[1].side").value("SELL"));

        verify(whaleEventRepository).getAll(100);
        verifyNoMoreInteractions(whaleEventRepository);
    }

    // /offchain/whales/asset/{asset}

    @Test
    void getWhalesByAsset_returns200AndList() throws Exception{
        when(whaleEventRepository.getAllByAsset(Asset.BTC, 100)).thenReturn(List.of(btcBinanceBuy));

        mockMvc.perform(get("/offchain/whales/asset/BTC"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(1))

                .andExpect(jsonPath("$[0].id").value("1"))
                .andExpect(jsonPath("$[0].exchange").value("BINANCE"))
                .andExpect(jsonPath("$[0].asset").value("BTC"));

        verify(whaleEventRepository).getAllByAsset(Asset.BTC, 100);
        verifyNoMoreInteractions(whaleEventRepository);
    }

    @Test
    void getWhalesByAsset_returns400ForInvalidAsset() throws Exception{
        mockMvc.perform(get("/offchain/whales/asset/INVALID_ASSET"))
                .andExpect(status().isBadRequest());

        verifyNoInteractions(whaleEventRepository);
    }

    // /offchain/whales/exchange/{exchange}

    @Test
    void getWhalesByExchange_returns200AndList() throws Exception{
        when(whaleEventRepository.getAllByExchange(Exchange.BINANCE, 100)).thenReturn(List.of(btcBinanceBuy));

        mockMvc.perform(get("/offchain/whales/exchange/BINANCE"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(1))

                .andExpect(jsonPath("$[0].id").value("1"))
                .andExpect(jsonPath("$[0].exchange").value("BINANCE"))
                .andExpect(jsonPath("$[0].asset").value("BTC"));

        verify(whaleEventRepository).getAllByExchange(Exchange.BINANCE, 100);
        verifyNoMoreInteractions(whaleEventRepository);
    }

    @Test
    void getWhalesByExchange_returns400ForInvalidExchange() throws Exception{
        mockMvc.perform(get("/offchain/whales/exchange/INVALID_EXCHANGE"))
                .andExpect(status().isBadRequest());

        verifyNoInteractions(whaleEventRepository);
    }

    // /offchain/whales/asset/{asset}/exchange/{exchange}

    @Test
    void getWhalesByAssetAndExchange_returns200AndList() throws Exception{
        when(whaleEventRepository.getByAssetAndExchange(Asset.BTC, Exchange.BINANCE, 100))
                .thenReturn(List.of(btcBinanceBuy));

        mockMvc.perform(get("/offchain/whales/asset/BTC/exchange/BINANCE"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(1))

                .andExpect(jsonPath("$[0].id").value("1"))
                .andExpect(jsonPath("$[0].exchange").value("BINANCE"))
                .andExpect(jsonPath("$[0].asset").value("BTC"));

        verify(whaleEventRepository).getByAssetAndExchange(Asset.BTC,Exchange.BINANCE, 100);
        verifyNoMoreInteractions(whaleEventRepository);
    }

    @Test
    void getWhalesByAssetAndExchange_returns400ForInvalidAsset() throws Exception{
        mockMvc.perform(get("/offchain/whales/asset/INVALID_ASSET/exchange/BINANCE"))
                .andExpect(status().isBadRequest());

        verifyNoInteractions(whaleEventRepository);
    }

    @Test
    void getWhalesByAssetAndExchange_returns400ForInvalidExchange() throws Exception{
        mockMvc.perform(get("/offchain/whales/asset/BTC/exchange/INVALID_EXCHANGE"))
                .andExpect(status().isBadRequest());

        verifyNoInteractions(whaleEventRepository);
    }
}