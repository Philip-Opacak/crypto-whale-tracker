package com.whalewatcher.repository;

import com.whalewatcher.domain.Asset;
import com.whalewatcher.domain.OnChainWhaleEvent;

import java.util.List;

public interface OnWhaleEventRepository {
    List<OnChainWhaleEvent> getAll(int limit);
    List<OnChainWhaleEvent> getAllByAsset(Asset asset, int limit);
}
