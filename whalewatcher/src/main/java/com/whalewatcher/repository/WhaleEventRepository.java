package com.whalewatcher.repository;

import com.whalewatcher.domain.Asset;
import com.whalewatcher.domain.Exchange;
import com.whalewatcher.domain.OffChainWhaleEvent;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface WhaleEventRepository {
    void save(OffChainWhaleEvent whaleEvent);

    List<OffChainWhaleEvent> getAll();
    List<OffChainWhaleEvent> getAllByAsset(Asset asset);
    List<OffChainWhaleEvent> getAllByExchange(Exchange exchange);
    List<OffChainWhaleEvent>  getByAssetAndExchange(Asset asset, Exchange exchange);

    List<OffChainWhaleEvent> getAll(int limit);
    List<OffChainWhaleEvent> getAllByAsset(Asset asset, int limit);
    List<OffChainWhaleEvent> getAllByExchange(Exchange exchange, int limit);
    List<OffChainWhaleEvent> getByAssetAndExchange(Asset asset, Exchange exchange, int limit);
}
