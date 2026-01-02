package com.whalewatcher.controller;

import com.whalewatcher.domain.Asset;
import com.whalewatcher.domain.Exchange;
import com.whalewatcher.domain.OffChainWhaleEvent;
import com.whalewatcher.repository.WhaleEventRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/offchain")
public class OffChainController {

    private final WhaleEventRepository whaleEventRepository;

    public OffChainController(WhaleEventRepository whaleEventRepository) {
        this.whaleEventRepository = whaleEventRepository;
    }

    // Get all whale events
    @GetMapping("/whales")
    public ResponseEntity<List<OffChainWhaleEvent>> getAll(
            @RequestParam(defaultValue = "100") int limit
    ) {
        return ResponseEntity.ok(whaleEventRepository.getAll(limit));
    }

    // Get whale events based on their asset
    @GetMapping("/whales/asset/{asset}")
    public ResponseEntity<List<OffChainWhaleEvent>> getByAsset(
            @PathVariable Asset asset,
            @RequestParam(defaultValue = "100") int limit
    ) {
        return ResponseEntity.ok(whaleEventRepository.getAllByAsset(asset, limit));
    }

    // Get whale events based on the exchange
    @GetMapping("/whales/exchange/{exchange}")
    public ResponseEntity<List<OffChainWhaleEvent>> getByExchange(
            @PathVariable Exchange exchange,
            @RequestParam(defaultValue = "100") int limit
    ) {
        return ResponseEntity.ok(whaleEventRepository.getAllByExchange(exchange, limit));
    }

    // Get whales events based on the asset and exchange
    @GetMapping("/whales/asset/{asset}/exchange/{exchange}")
    public ResponseEntity<List<OffChainWhaleEvent>> getByAssetAndExchange(
            @PathVariable Asset asset,
            @PathVariable Exchange exchange,
            @RequestParam(defaultValue = "100") int limit
    ) {
        return ResponseEntity.ok(whaleEventRepository.getByAssetAndExchange(asset, exchange, limit));
    }
}