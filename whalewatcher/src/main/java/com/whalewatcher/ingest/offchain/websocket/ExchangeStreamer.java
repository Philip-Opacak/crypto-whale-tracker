package com.whalewatcher.ingest.offchain.websocket;

import com.whalewatcher.domain.Exchange;

public interface ExchangeStreamer {
    Exchange exchange();
    void start();
    void stop();
}
