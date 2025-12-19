package com.whalewatcher.controller;

import com.whalewatcher.domain.OnChainWhaleEvent;
import com.whalewatcher.ingest.onchain.evm.OnChainEventBuffer;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/onchain")
@RequiredArgsConstructor
public class OnChainController {
    private final OnChainEventBuffer buffer;

    @GetMapping("/whales")
    public List<OnChainWhaleEvent> whales(@RequestParam(defaultValue = "50") int limit) {
        return buffer.latest(limit);
    }
}
