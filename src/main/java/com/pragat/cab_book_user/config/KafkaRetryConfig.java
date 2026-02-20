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
                    String dltTopic = record.topic().endsWith(".DLT") ?  record.topic() : record.topic()+".DLT";

                    return new TopicPartition(dltTopic, record.partition());

                });

        // Retry 3 times with 2s backoff
        FixedBackOff fixedBackOff = new FixedBackOff(2000L, 3L);

        DefaultErrorHandler  defaultErrorHandler = new DefaultErrorHandler(deadLetterPublishingRecoverer, fixedBackOff);

        // ✅ M12: mark exceptions that should NOT be retried (go straight to DLT)
        defaultErrorHandler.addNotRetryableExceptions(
                IllegalArgumentException.class,
                com.fasterxml.jackson.core.JsonProcessingException.class
        );

        defaultErrorHandler.setRetryListeners((record, ex, deliveryAttempt) -> {
            // deliveryAttempt starts at 1
            System.out.println("Retry attempt " + deliveryAttempt +
                    " for topic=" + record.topic() +
                    " partition=" + record.partition() +
                    " offset=" + record.offset() +
                    " due to: " + ex.getClass().getSimpleName() + " - " + ex.getMessage());
        });

        return defaultErrorHandler ;
    }

}
