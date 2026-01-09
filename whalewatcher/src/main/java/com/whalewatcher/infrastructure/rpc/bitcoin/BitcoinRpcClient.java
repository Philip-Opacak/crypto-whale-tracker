package com.whalewatcher.infrastructure.rpc.bitcoin;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class BitcoinRpcClient {

    private final WebClient webClient;
    private final AtomicLong rpcId = new AtomicLong(1);

    public BitcoinRpcClient(
            WebClient.Builder builder,
            @Value("${whalewatcher.onchain.quicknodeBtcHttpUrl}") String url
    ) {
        if (url == null || url.isBlank()) {
            throw new IllegalStateException("Missing whalewatcher.onchain.quicknodeBtcHttpUrl / QUICKNODE_BTC_HTTP_URL");
        }
        this.webClient = builder.baseUrl(url).build();
    }

    // get the latest BTC block height
    public long getBlockCount() {
        Map<?, ?> response = postRpc("getblockcount", List.of());
        Object result = response.get("result");
        if (result instanceof Number n) return n.longValue();
        if (result instanceof String s) return Long.parseLong(s);
        throw new IllegalStateException("Unexpected getblockcount result: " + result);
    }

    // get the block hash for height
    public String getBlockHash(long height) {
        Map<?, ?> response = postRpc("getblockhash", List.of(height));
        Object result = response.get("result");
        if (result instanceof String s && !s.isBlank()) return s;
        throw new IllegalStateException("Unexpected getblockhash result: " + result);
    }

    // get the decoded block
    public Map<?, ?> getBlockVerbose2(String blockHash) {
        return postRpc("getblock", List.of(blockHash, 2));
    }

    // Send a rpc request to Bitcoin (QuickNode)
    private Map<?, ?> postRpc(String method, List<?> params) {
        long id = rpcId.getAndIncrement();

        Map<String, Object> body = Map.of(
                "jsonrpc", "2.0",
                "id", id,
                "method", method,
                "params", params
        );

        Map<?, ?> resp = webClient.post()
                .bodyValue(body)
                .retrieve()
                .bodyToMono(Map.class)
                .timeout(Duration.ofSeconds(10))
                .retryWhen(
                        Retry.backoff(2, Duration.ofMillis(300))
                                .maxBackoff(Duration.ofSeconds(2))
                                .filter(ex -> {
                                    if (ex instanceof TimeoutException) return true;
                                    if (ex instanceof WebClientResponseException wex) {
                                        int s = wex.getStatusCode().value();
                                        return s == 429 || (s >= 500 && s <= 599);
                                    }
                                    return false;
                                })
                )
                .block();

        if (resp == null) {
            throw new IllegalStateException("Null RPC response for " + method);
        }

        if (resp.containsKey("error")) {
            throw new IllegalStateException("RPC error for " + method + ": " + resp.get("error"));
        }

        return resp;
    }
}