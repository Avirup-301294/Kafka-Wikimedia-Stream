package com.kafkaConsumer.springboot.kafka;

import com.kafkaConsumer.springboot.model.WikimediaData;
import com.kafkaConsumer.springboot.repository.WikimediaDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class WikimediaChangesConsumer {
    private static final Logger logger = LoggerFactory.getLogger(WikimediaChangesConsumer.class);
    @Autowired
    private WikimediaDataRepository wikimediaDataRepository;

    @KafkaListener(topics = "wikimedia_recent_change", groupId = "myGroup")
    private void consume(String eventMessage) {
        logger.info(String.format("Event message Received -> %s", eventMessage));
        WikimediaData wikimediaData = new WikimediaData();
        wikimediaData.setWikiEventData(eventMessage);

        wikimediaDataRepository.save(wikimediaData);
    }
}
