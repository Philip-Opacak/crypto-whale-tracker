package com.whalewatcher.service;

import com.whalewatcher.domain.Trade;
import com.whalewatcher.repository.WhaleEventRepository;
import org.springframework.stereotype.Service;

@Service
public class IngestionService {
    private final NormalizationService normalizationService;
    private final WhaleEventRepository whaleEventRepository;

    public IngestionService(NormalizationService normalizationService, WhaleEventRepository whaleEventRepository) {
        this.normalizationService = normalizationService;
        this.whaleEventRepository = whaleEventRepository;
    }

    public void ingest(Trade trade){
        normalizationService.normalizeAndFilter(trade)
                .ifPresent(whaleEventRepository::save);
    }
}