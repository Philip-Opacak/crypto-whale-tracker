package com.whalewatcher.ingest.onchain.bitcoin;


import com.whalewatcher.domain.Asset;
import com.whalewatcher.domain.Chain;
import com.whalewatcher.domain.OnChainWhaleEvent;
import com.whalewatcher.infrastructure.rpc.bitcoin.BitcoinRpcClient;
import com.whalewatcher.repository.OnChainEventBuffer;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@Component
@RequiredArgsConstructor
public class BitcoinWhaleWatcher {

    private final BitcoinRpcClient btcRpcClient;
    private final OnChainEventBuffer buffer;

    @Value("${whalewatcher.onchain.native.whaleThresholdBtc:100}")
    private BigDecimal whaleThresholdBtc;

    @Value("${whalewatcher.onchain.maxBlocksPerPoll:50}")
    private long maxBlocksPerPoll;

    private long lastProcessedBlock = -1;

    // poll api process latest blocks
    @Scheduled(fixedDelayString = "${whalewatcher.onchain.pollMs:60000}")
    public void poll() {
        long latest = btcRpcClient.getBlockCount();

        if (lastProcessedBlock < 0) {
            lastProcessedBlock = latest - 10;
        }

        long to = Math.min(latest, lastProcessedBlock + maxBlocksPerPoll);

        for (long height = lastProcessedBlock + 1; height <= to; height++) {
            try {
                String blockHash = btcRpcClient.getBlockHash(height);
                Map<?, ?> resp = btcRpcClient.getBlockVerbose2(blockHash);

                Object resultObj = resp.get("result");
                if (!(resultObj instanceof Map<?, ?> block)) continue;

                Object txObj = block.get("tx");
                if (!(txObj instanceof List<?> txs)) continue;

                for (Object t : txs) {
                    if (!(t instanceof Map<?, ?> tx)) continue;

                    String txid = (String) tx.get("txid");
                    if (txid == null) continue;

                    Object voutObj = tx.get("vout");
                    if (!(voutObj instanceof List<?> vouts)) continue;

                    for (Object vo : vouts) {
                        if (!(vo instanceof Map<?, ?> vout)) continue;

                        BigDecimal valueBtc = parseBtcValue(vout.get("value"));
                        if (valueBtc == null) continue;
                        if (valueBtc.compareTo(whaleThresholdBtc) < 0) continue;

                        String toAddr = extractToAddress(vout.get("scriptPubKey"));

                        buffer.add(new OnChainWhaleEvent(
                                Chain.BITCOIN,
                                Asset.BTC,
                                valueBtc,
                                null,        // fromAddr unknown in UTXO model without extra lookups
                                toAddr,
                                txid,
                                height,
                                System.currentTimeMillis()
                        ));
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        lastProcessedBlock = to;
    }

    private BigDecimal parseBtcValue(Object valueObj) {
        // btc RPC returns a json double for "value"
        if (valueObj instanceof Number n) {
            return new BigDecimal(n.toString());
        }
        if (valueObj instanceof String s) {
            try {
                return new BigDecimal(s);
            } catch (Exception ignored) {
            }
        }
        return null;
    }

    /* extractToAddress() tries to extract the recipient Bitcoin address from a transaction output’s
     * scriptPubKey, handling both the modern "address" field and the older "addresses" list formats.
     * If no clear address can be determined returns null.
     */
    private String extractToAddress(Object scriptPubKeyObj) {
        if (!(scriptPubKeyObj instanceof Map<?, ?> spk)) return null;

        Object address = spk.get("address");
        if (address instanceof String s) return s;

        Object addresses = spk.get("addresses");
        if (addresses instanceof List<?> list && !list.isEmpty() && list.get(0) instanceof String s) return s;

        return null;
    }
}