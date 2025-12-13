package com.whalewatcher.repository;

import com.whalewatcher.domain.WhaleEvent;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface WhaleEventRepository {
    void save(WhaleEvent whaleEvent);

    List<WhaleEvent> findRecent(int limit);
}
