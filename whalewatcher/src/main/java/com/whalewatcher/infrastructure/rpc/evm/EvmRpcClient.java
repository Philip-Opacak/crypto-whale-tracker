package com.whalewatcher.infrastructure.rpc.evm;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;


import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class EvmRpcClient {

    private final WebClient webClient;
    private final AtomicLong rpcId = new AtomicLong(1);

    public EvmRpcClient(
            WebClient.Builder builder,
            @Value("${whalewatcher.onchain.quicknodeHttpUrl}") String url
    ) {
        if (url == null || url.isBlank()) {
            throw new IllegalStateException("Missing whalewatcher.onchain.quicknodeHttpUrl / QUICKNODE_HTTP_URL");
        }
        this.webClient = builder.baseUrl(url).build();
    }

    // Get the latest ETH block number (height)
    public long ethBlockNumber() {
        Map<?, ?> response = postRpc("eth_blockNumber", List.of());
        String hex = (String) response.get("result");
        if (hex == null || !hex.startsWith("0x")) {
            throw new IllegalStateException("Unexpected eth_blockNumber result: " + hex);
        }
        return Long.parseLong(hex.substring(2), 16);
    }

    // Get transactions inside block n
    public Map<?, ?> getBlockByNumber(long blockNumber) {
        String hexBlock = "0x" + Long.toHexString(blockNumber);
        return postRpc("eth_getBlockByNumber", List.of(hexBlock, true));
    }

    //Send a JSON-RPC request to Ethereum
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
                .block();

        if (resp == null) throw new IllegalStateException("Null RPC response for " + method);

        if (resp.containsKey("error")) {
            throw new IllegalStateException("RPC error for " + method + ": " + resp.get("error"));
        }

        return resp;
    }
}
