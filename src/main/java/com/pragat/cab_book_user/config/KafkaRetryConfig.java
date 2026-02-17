package com.pragat.cab_book_user.config;

import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaRetryConfig {


    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> kafkaTemplate) {

        // Send to <topic>.DLT automatically
        DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = new DeadLetterPublishingRecoverer
                (kafkaTemplate, (record, ex) -> {
                    String dltTopic = record.topic().endsWith("DLT") ?  record.topic() : record.topic()+".DLT";

                    return new TopicPartition(dltTopic, record.partition());

                });

        // Retry 3 times with 2s backoff
        FixedBackOff fixedBackOff = new FixedBackOff(2000L, 3L);

        return new DefaultErrorHandler(deadLetterPublishingRecoverer, fixedBackOff);
    }

}
