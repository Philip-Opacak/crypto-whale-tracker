package com.whalewatcher.controller;

import com.whalewatcher.domain.Trade;
import com.whalewatcher.domain.OffChainWhaleEvent;
import com.whalewatcher.repository.WhaleEventRepository;
import com.whalewatcher.service.IngestionService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api")
public class WhaleController {

    private final IngestionService ingestionService;
    private final WhaleEventRepository whaleEventRepository;

    public WhaleController(IngestionService ingestionService, WhaleEventRepository whaleEventRepository) {
        this.ingestionService = ingestionService;
        this.whaleEventRepository = whaleEventRepository;
    }

    @PostMapping("/trades")
    public ResponseEntity<Void> ingest(@RequestBody Trade trade){
        ingestionService.ingest(trade);
        return ResponseEntity.accepted().build();
    }

    @GetMapping("/whales/recent")
    public List<OffChainWhaleEvent> findRecent(@RequestParam(defaultValue = "50") int limit){
        return whaleEventRepository.findRecent(limit);
    }
}
