package com.whalewatcher.ingest.onchain.evm;

import com.whalewatcher.domain.Asset;
import com.whalewatcher.domain.Chain;
import com.whalewatcher.domain.OnChainWhaleEvent;
import com.whalewatcher.infrastructure.rpc.evm.EvmRpcClient;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

@Component
@RequiredArgsConstructor
public class EthereumWhaleWatcher {

    private final EvmRpcClient evmRpcClient;
    private final OnChainEventBuffer buffer;

    @Value("${whalewatcher.onchain.native.whaleThresholdEth:500}")
    private BigDecimal whaleThresholdEth;

    private long lastProcessedBlock = -1;

    @Scheduled(fixedDelayString = "${whalewatcher.onchain.pollMs:60000}")
    public void poll() {
        long latest = evmRpcClient.ethBlockNumber();

        if(lastProcessedBlock < 0){
            lastProcessedBlock = latest - 10;
        }

        // Cap number of blocks processed per minute
        long maxBlocks = 50;
        long to = Math.min(latest, lastProcessedBlock + maxBlocks);

        //Fetch each block in order
        for (long b = lastProcessedBlock + 1; b <= to; b++) {
            try {
                Map<?, ?> resp = evmRpcClient.getBlockByNumber(b);
                Object resultObj = resp.get("result");
                if (!(resultObj instanceof Map<?, ?> block)) continue;

                Object txObj = block.get("transactions");
                if (!(txObj instanceof List<?> txs)) continue;

                for (Object t : txs) {
                    if (!(t instanceof Map<?, ?> tx)) continue;

                    String valueHex = (String) tx.get("value");
                    if (valueHex == null || !valueHex.startsWith("0x")) continue;

                    BigInteger wei = new BigInteger(valueHex.substring(2), 16);
                    if (wei.signum() <= 0) continue;

                    BigDecimal eth = new BigDecimal(wei).movePointLeft(18);

                    if (eth.compareTo(whaleThresholdEth) >= 0) {
                        String fromAddr = (String) tx.get("from");
                        // "to" can be null (contract creation) but will not if regular transaction
                        String toAddr = (String) tx.get("to");
                        String hash = (String) tx.get("hash");
                        if (hash == null) continue;

                        buffer.add(new OnChainWhaleEvent(
                                Chain.ETHEREUM, Asset.ETH, eth,
                                fromAddr, toAddr, hash,
                                b, System.currentTimeMillis()
                        ));
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        lastProcessedBlock = to;
    }
}