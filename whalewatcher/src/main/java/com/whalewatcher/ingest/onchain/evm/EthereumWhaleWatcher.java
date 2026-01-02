package com.whalewatcher.ingest.onchain.evm;

import com.whalewatcher.domain.Asset;
import com.whalewatcher.domain.Chain;
import com.whalewatcher.domain.OnChainWhaleEvent;
import com.whalewatcher.infrastructure.rpc.evm.EvmRpcClient;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class EthereumWhaleWatcher {

    private final EvmRpcClient evmRpcClient;
    private final OnChainEventBuffer buffer;

    @Value("${whalewatcher.onchain.native.whaleThresholdEth:500}")
    private double whaleThresholdEth;

    private long lastProcessedBlock = -1;

    @Scheduled(fixedDelayString = "${whalewatcher.onchain.pollMs:60000}")
    public void poll() {
        long latest = evmRpcClient.ethBlockNumber();

        if(lastProcessedBlock < 0){
            lastProcessedBlock = latest;
            return;
        }

        // Cap number of blocks processed per minute
        long maxBlocks = 50;
        long to = Math.min(latest, lastProcessedBlock + maxBlocks);

        //Fetch each block in order
        for(long b = lastProcessedBlock + 1; b <= to; b++){
            // Map of keys and values of block from Ethereum
            Map<?,?> block = evmRpcClient.getBlockByNumber(b);

            //Get the transactions from the block
            Object txObj = block.get("transactions");

            // If the transactions is not a list, skip this block
            if (!(txObj instanceof List<?> txs)) continue;

            for (Object t : txs) {
                if (!(t instanceof Map<?, ?> tx)) continue;

                String valueHex = (String) tx.get("value");
                if (valueHex == null || !valueHex.startsWith("0x")) continue;

                BigInteger wei = new BigInteger(valueHex.substring(2), 16);
                if (wei.signum() <= 0) continue;

                double eth = wei.doubleValue() / 1e18;

                if (eth >= whaleThresholdEth) {
                    String fromAddr = (String) tx.get("from");
                    String toAddr = (String) tx.get("to");
                    String hash = (String) tx.get("hash");

                    buffer.add(new OnChainWhaleEvent(
                            UUID.randomUUID().toString(),
                            Chain.ETHEREUM,
                            Asset.ETH,
                            eth,
                            fromAddr,
                            toAddr,
                            hash,
                            b,
                            System.currentTimeMillis()
                    ));
                }
            }
        }
        lastProcessedBlock = to;
    }
}