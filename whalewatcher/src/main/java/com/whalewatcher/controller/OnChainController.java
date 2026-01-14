package com.whalewatcher.controller;

import com.whalewatcher.domain.Asset;
import com.whalewatcher.domain.OnChainWhaleEvent;
import com.whalewatcher.repository.OnChainEventBuffer;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/onchain")
@RequiredArgsConstructor
public class OnChainController {
    private final OnChainEventBuffer buffer;

    @GetMapping("/whales")
    public List<OnChainWhaleEvent> whales(@RequestParam(defaultValue = "50") int limit) {
        int safeLimit = Math.max(1, Math.min(limit, 500));
        return buffer.getAll(safeLimit);
    }

    @GetMapping("whales/asset/{asset}")
    public ResponseEntity<List<OnChainWhaleEvent>> getByAsset(
            @PathVariable Asset asset,
            @RequestParam(defaultValue = "100") int limit
    ) {
        return ResponseEntity.ok(buffer.getAllByAsset(asset, limit));
    }
}