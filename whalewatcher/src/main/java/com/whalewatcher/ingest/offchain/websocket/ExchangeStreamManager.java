package com.whalewatcher.ingest.offchain.websocket;

import com.whalewatcher.domain.Exchange;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Component
public class ExchangeStreamManager {

    private final List<ExchangeStreamer> streamers;
    private final StreamProperties props;

    public  ExchangeStreamManager(List<ExchangeStreamer> streamers, StreamProperties props) {
        this.streamers = streamers;
        this.props = props;
    }

    @PostConstruct
    public void startEnabled() {
        Set<Exchange> enabled = props.getEnabled().stream().collect(Collectors.toSet());

        streamers.stream()
                .filter(s -> enabled.contains(s.exchange()))
                .forEach(ExchangeStreamer::start);

        System.out.println("Started exchange streams: " + enabled);
    }

    @PreDestroy
    public void stopAll() {
        streamers.forEach(ExchangeStreamer::stop);
        System.out.println("Stopped all exchange streams");
    }
}
