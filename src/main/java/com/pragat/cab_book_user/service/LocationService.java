package com.pragat.cab_book_user.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pragat.cab_book_user.LocationEvent;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Service
public class LocationService {

    @Value("${app.instance-id}")
    private String instanceId;

    private final ObjectMapper mapper = new ObjectMapper();

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private KafkaTemplate<String,Object> kafkaTemplate;

    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT = new DefaultRedisScript<>(
            "if redis.call('GET', KEYS[1]) == ARGV[1] then " +
                    "  return redis.call('DEL', KEYS[1]) " +
                    "else " +
                    "  return 0 " +
                    "end",
            Long.class
    );



    Logger log = LoggerFactory.getLogger(LocationService.class);

    // Commented because using redis to store for dedupe
//    private final Set<String> processedEventIds =
//            ConcurrentHashMap.newKeySet();

    @KafkaListener(topics = "Cab-Location-Latest", groupId = "user-group")
    public void cabLocation(@Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                            @Header(KafkaHeaders.RECEIVED_KEY) String cabId,
                            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                            @Header(KafkaHeaders.OFFSET) long offset,
                            LocationEvent locationEvent) throws JsonProcessingException, InterruptedException {

        String lockKey = lockKey(locationEvent.eventId());
        String token = instanceId + ":" + UUID.randomUUID();

        boolean success = false;

        String processedKey = processedKey(locationEvent.eventId());

        ConsumerGroupMetadata consumerGroupMetadata = new ConsumerGroupMetadata("user-group");

        // Acquire lock (NX + TTL)
        Boolean locked = redisTemplate.opsForValue()
                .setIfAbsent(lockKey, token, Duration.ofSeconds(5));


        if (Boolean.FALSE.equals(locked)) {
            log.info("LOCKED; will retry. instance={} eventId={} partition={} offset={}",
                    instanceId, locationEvent.eventId(), partition, offset);
            throw new RuntimeException("Lock held; retry");
        }

        log.info("LOCK ACQUIRED instance={} eventId={} token={}", instanceId, locationEvent.eventId(), token);

//        String processingKey = processingKey(locationEvent.eventId());


//        Boolean locked = redisTemplate.opsForValue().setIfAbsent(processingKey, instanceId, Duration.ofSeconds(90));
//
//        if (!locked) {
//            System.out.println("SKIP already being processed eventId=" + locationEvent.eventId());
//            ack.nack(Duration.ofSeconds(5));
//            return;
//        }


        // Commented because we dont want to mix the event that are processing and that are processed.


        try {

            String pKey = processedKey(locationEvent.eventId());
            boolean firstTime = redisTemplate.opsForValue().setIfAbsent(pKey,"1", Duration.ofHours(24));

            if (Boolean.FALSE.equals(firstTime)) {
                kafkaTemplate.executeInTransaction(kt -> {
                    kt.sendOffsetsToTransaction(offsetMap(topic, partition, offset), consumerGroupMetadata);
                    return true;
                });
                log.info("SKIP+COMMIT(TX) instance={} partition={} offset={} eventId={}",
                        instanceId, partition, offset, locationEvent.eventId());
                return;
            }

//            if (redisTemplate.hasKey(processedKey)) {
//                System.out.println("SKIP already processed eventId=" + locationEvent.eventId());
//                ack.acknowledge();
//                redisTemplate.delete(processingKey);
//                success = true;
//                return;
//            }

//              log.info("SLEEP START eventId={} time={}", locationEvent.eventId(), System.currentTimeMillis());
//
//              Thread.sleep(2000);
//
//              log.info("SLEEP END eventId={} time={}", locationEvent.eventId(), System.currentTimeMillis());
//
////            if (locationEvent.cabId().equals("CAB-102") && locationEvent.seq() % 3 == 0) {
////                throw new RuntimeException("Simulated failure for testing retries");
////            }
//
//            System.out.println("Instance=" + instanceId
//                    + " partition=" + partition
//                    + " cab=" + locationEvent.cabId()
//                    + " seq=" + locationEvent.seq());
//
//            String locationKey = cabLocKey(locationEvent.cabId());
//
//            redisTemplate.opsForHash().put(locationKey, "lat", String.valueOf(locationEvent.lat()));
//            redisTemplate.opsForHash().put(locationKey, "lon", String.valueOf(locationEvent.lon()));
//            redisTemplate.opsForHash().put(locationKey, "ts", String.valueOf(locationEvent.timestamp()));
//            redisTemplate.opsForHash().put(locationKey, "seq", String.valueOf(locationEvent.seq()));
//            redisTemplate.opsForHash().put(locationKey, "eventId", String.valueOf(locationEvent.eventId()));
//            redisTemplate.expire(locationKey, 1, TimeUnit.DAYS);
//
//            redisTemplate.opsForGeo().add("cab:geo",new Point(locationEvent.lon(),locationEvent.lat()),locationEvent.cabId());
//
//            //redisTemplate.opsForValue().set(processedKey, "1", java.time.Duration.ofHours(24));
//
//            //System.out.println("PROCESSED partition=" + partition + " cab=" + locationEvent.cabId() + " seq=" + locationEvent.seq());
//
//            log.info("PROCESSED instance={} partition={} eventId={} cab={} seq={}",
//                    instanceId, partition, locationEvent.eventId(), locationEvent.cabId(), locationEvent.seq());

            kafkaTemplate.executeInTransaction(kt -> {

                log.info("SLEEP START eventId={} time={}", locationEvent.eventId(), System.currentTimeMillis());
                try {
                    Thread.sleep(8000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                log.info("SLEEP END eventId={} time={}", locationEvent.eventId(), System.currentTimeMillis());

                // 1) Redis materialized view (if Redis fails, throw -> tx aborts -> retry)
                String locationKey = cabLocKey(locationEvent.cabId());

                redisTemplate.opsForHash().put(locationKey, "lat", String.valueOf(locationEvent.lat()));
                redisTemplate.opsForHash().put(locationKey, "lon", String.valueOf(locationEvent.lon()));
                redisTemplate.opsForHash().put(locationKey, "ts", String.valueOf(locationEvent.timestamp()));
                redisTemplate.opsForHash().put(locationKey, "seq", String.valueOf(locationEvent.seq()));
                redisTemplate.opsForHash().put(locationKey, "eventId", String.valueOf(locationEvent.eventId()));
                redisTemplate.expire(locationKey, 1, TimeUnit.DAYS);

                redisTemplate.opsForGeo().add("cab:geo",
                        new Point(locationEvent.lon(), locationEvent.lat()),
                        locationEvent.cabId());

                // 2) Produce derived event (proof topic for M15)
                kt.send("Cab-Location-Enriched", locationEvent.cabId(), locationEvent);

                if (locationEvent.seq() == 7) throw new RuntimeException("M15 crash test");

                // 3) Commit the consumed offset inside the same transaction
                kt.sendOffsetsToTransaction(offsetMap(topic, partition, offset), consumerGroupMetadata);

                return true;
            });

            log.info("PROCESSED+SENT+COMMIT(TX) instance={} partition={} offset={} eventId={} cab={} seq={}",
                    instanceId, partition, offset, locationEvent.eventId(), locationEvent.cabId(), locationEvent.seq());



        } catch (Exception e) {
            redisTemplate.delete(processedKey);
            throw new RuntimeException(e);
        } finally {
            try {
                // Safe unlock: delete only if value still equals token
                Long deleted = redisTemplate.execute(
                        UNLOCK_SCRIPT,
                        Collections.singletonList(lockKey),
                        token
                );
                log.info("UNLOCK eventId={} deleted={}", locationEvent.eventId(), deleted);
            } catch (Exception ignored) {
            }
        }
    }

    @KafkaListener(topics = "Cab-Location-Latest.DLT", groupId = "user-group-dlt")
    public void onDlt(LocationEvent locationEvent) {

        System.out.println("DLT Received for " + locationEvent);

    }

    private java.util.Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata>
    offsetMap(String topic, int partition, long offset) {
        return java.util.Map.of(
                new org.apache.kafka.common.TopicPartition(topic, partition),
                new org.apache.kafka.clients.consumer.OffsetAndMetadata(offset + 1)
        );
    }

    private String processedKey(String eventId) {
        return "event:processed:" + eventId;
    }
    private String processingKey(String eventId) {
        return "event:processing:" + eventId;
    }


    private String cabLocKey(String cabId) {
        return "cab:loc:" + cabId;
    }

    private String lockKey(String eventId) {
        return "event:lock:" + eventId;
    }
}
