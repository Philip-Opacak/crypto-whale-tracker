package com.whalewatcher.ingest.offchain.websocket;

import com.whalewatcher.domain.Exchange;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@ConfigurationProperties(prefix = "streams")
public class StreamProperties {

    private List<Exchange> enabled = new ArrayList<>();

    public List<Exchange> getEnabled() {
        return enabled;
    }

    public void setEnabled(List<Exchange> enabled) {
        this.enabled = enabled;
    }
}
