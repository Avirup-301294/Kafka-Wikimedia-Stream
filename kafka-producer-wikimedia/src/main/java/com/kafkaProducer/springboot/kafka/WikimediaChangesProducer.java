package com.kafkaProducer.springboot.kafka;

import com.kafkaProducer.springboot.handler.WikimediaChangesHandler;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.StreamException;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.util.concurrent.TimeUnit;

@Service
public class WikimediaChangesProducer {

    private static final Logger Logger = LoggerFactory.getLogger(WikimediaChangesProducer.class);
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage() throws InterruptedException, StreamException {
        String topic = "wikimedia_recent_change";

        // to read real time stream data from wikimedia, we use event source
        BackgroundEventHandler eventHandler = new WikimediaChangesHandler(kafkaTemplate, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        EventSource.Builder eventSourceBuilder = new EventSource.Builder(URI.create(url));
        EventSource eventSource = eventSourceBuilder.build();
        eventSource.start();

        BackgroundEventSource.Builder backgroundEventSourceBuilder = new BackgroundEventSource.Builder(eventHandler, eventSourceBuilder);
        BackgroundEventSource backgroundEventSource = backgroundEventSourceBuilder.build();
        backgroundEventSource.start();

        TimeUnit.MINUTES.sleep(10);
    }

}
