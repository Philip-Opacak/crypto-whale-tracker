package com.whalewatcher.repository;

import com.whalewatcher.domain.OffChainWhaleEvent;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface WhaleEventRepository {
    void save(OffChainWhaleEvent whaleEvent);

    List<OffChainWhaleEvent> findRecent(int limit);
}
