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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class LocationService {

    @Value("${app.instance-id}")
    private String instanceId;

    private final ObjectMapper mapper = new ObjectMapper();

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    // Return codes:
//  1  = deleted (token matched)
// -1  = token mismatch (someone else owns it)
// -2  = key missing (expired / already gone)
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT = new DefaultRedisScript<>(
            "local v = redis.call('GET', KEYS[1]); " +
                    "if not v then return -2 end; " +
                    "if v == ARGV[1] then return redis.call('DEL', KEYS[1]) else return -1 end;",
            Long.class
    );

    private static final DefaultRedisScript<Long> UPDATE_IF_NEWER_SEQ_SCRIPT = new DefaultRedisScript<>(

            """
                    -- M17.2: atomic idempotent update for cab:loc:<cabId>
                    -- Only apply if incomingSeq > storedSeq.
                    -- Returns: 1 applied, 0 skipped.
                    
                    local key = KEYS[1]
                    
                    local incomingSeq = tonumber(ARGV[1])
                    if not incomingSeq then
                      return redis.error_reply("incomingSeq must be a number")
                    end
                    
                    -- stored seq might be missing or non-numeric
                    local storedSeqRaw = redis.call("HGET", key, "seq")
                    local storedSeq = tonumber(storedSeqRaw)
                    
                    -- If storedSeq exists and is >= incoming, skip
                    if storedSeq and storedSeq >= incomingSeq then
                      return 0
                    end
                    
                    -- Apply the update
                    redis.call("HSET", key,
                      "lat", ARGV[2],
                      "lon", ARGV[3],
                      "ts",  ARGV[4],
                      "seq", ARGV[1],
                      "eventId", ARGV[5]
                    )
                    
                    -- Keep the view alive
                    local ttlSeconds = tonumber(ARGV[6])
                    if ttlSeconds and ttlSeconds > 0 then
                      redis.call("EXPIRE", key, ttlSeconds)
                    end
                    
                    return 1
                    """,
            Long.class

    );

    // 1 = renewed, 0 = not renewed (missing or token mismatch)
    private static final DefaultRedisScript<Long> RENEW_SCRIPT = new DefaultRedisScript<>(
            "if redis.call('GET', KEYS[1]) == ARGV[1] then " +
                    "  return redis.call('PEXPIRE', KEYS[1], ARGV[2]) " +
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
                            Acknowledgment ack,
                            LocationEvent locationEvent) throws JsonProcessingException, InterruptedException {

        String lockKey = "cab:lock:" + locationEvent.cabId();   // string key for lock
        String token = instanceId + ":" + UUID.randomUUID();

        final long ttlMs = 5000;

        AtomicBoolean lockLost = new AtomicBoolean(false);

//        String processedKey = processedKey(locationEvent.eventId());

        ConsumerGroupMetadata consumerGroupMetadata = new ConsumerGroupMetadata("user-group");

        // Acquire lock (NX + TTL)
        Boolean locked = redisTemplate.opsForValue()
                .setIfAbsent(lockKey, token, Duration.ofMillis(ttlMs));

        boolean lockAcquired = Boolean.TRUE.equals(locked);

        if (!lockAcquired) {
            log.info("LOCKED; will retry. instance={} cab={} eventId={} partition={} offset={}",
                    instanceId, locationEvent.cabId(), locationEvent.eventId(), partition, offset);
            ack.nack(Duration.ofMillis(2000));
            return;
        }

        log.info("LOCK ACQUIRED instance={} cab={} eventId={} token={}",
                instanceId, locationEvent.cabId(), locationEvent.eventId(), token);

//        String processingKey = processingKey(locationEvent.eventId());


//        Boolean locked = redisTemplate.opsForValue().setIfAbsent(processingKey, instanceId, Duration.ofSeconds(90));
//
//        if (!locked) {
//            System.out.println("SKIP already being processed eventId=" + locationEvent.eventId());
//            ack.nack(Duration.ofSeconds(5));
//            return;
//        }


        // Commented because we dont want to mix the event that are processing and that are processed.


        final long renewEveryMs = 2000;

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        ScheduledFuture<?> renewFuture = scheduler.scheduleAtFixedRate(() -> {

            try {

                if (lockLost.get()) return;

                Long renewed = redisTemplate.execute(
                        RENEW_SCRIPT,
                        Collections.singletonList(lockKey),
                        token,
                        String.valueOf(ttlMs)
                );

                if (renewed == null || renewed == 0L) {
                    String current = redisTemplate.opsForValue().get(lockKey);
                    if (!token.equals(current)) {
                        lockLost.set(true);
                        log.info("LOCK LOST instance={} cab={} eventId={}",
                                instanceId, locationEvent.cabId(), locationEvent.eventId());
                    }else{
                        log.debug("LOCK RENEW returned 0 but token still matches; ignoring. eventId={}", locationEvent.eventId());
                    }
                } else {
                    log.debug("LOCK RENEW OK instance={} eventId={}", instanceId, locationEvent.eventId());
                }
            } catch (Exception e) {
                log.warn("LOCK RENEW ERROR instance={} eventId={} err={}", instanceId, locationEvent.eventId(), e.toString());
            }

        }, renewEveryMs, renewEveryMs, TimeUnit.MILLISECONDS);

        try {

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
                ensureLockStillOwned(lockLost);
                try {
                    Thread.sleep(4500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                log.info("SLEEP END eventId={} time={}", locationEvent.eventId(), System.currentTimeMillis());

                ensureLockStillOwned(lockLost);


                // 1) Redis materialized view (if Redis fails, throw -> tx aborts -> retry)
                String locationKey = cabLocKey(locationEvent.cabId());

                Long applied = redisTemplate.execute(UPDATE_IF_NEWER_SEQ_SCRIPT, Collections.singletonList(locationKey),
                        String.valueOf(locationEvent.seq()),
                        String.valueOf(locationEvent.lat()),
                        String.valueOf(locationEvent.lon()),
                        String.valueOf(locationEvent.timestamp()),
                        String.valueOf(locationEvent.eventId()),
                        "86400");

                log.info("M17.2 APPLY result={} cab={} seq={} eventId={}", applied, locationEvent.cabId(), locationEvent.seq(), locationEvent.eventId());

                // Commented becasue we dont want to  unconditionally apply the below changes and only apply them they are newer

//                redisTemplate.opsForHash().put(locationKey, "lat", String.valueOf(locationEvent.lat()));
//                redisTemplate.opsForHash().put(locationKey, "lon", String.valueOf(locationEvent.lon()));
//                redisTemplate.opsForHash().put(locationKey, "ts", String.valueOf(locationEvent.timestamp()));
//                redisTemplate.opsForHash().put(locationKey, "seq", String.valueOf(locationEvent.seq()));
//                redisTemplate.opsForHash().put(locationKey, "eventId", String.valueOf(locationEvent.eventId()));
//                redisTemplate.expire(locationKey, 1, TimeUnit.DAYS);

                if (applied == null) {
                    throw new RuntimeException("Lua returned null");
                }

                if (applied == 1L) {

                    redisTemplate.opsForGeo().add("cab:geo",
                            new Point(locationEvent.lon(), locationEvent.lat()),
                            locationEvent.cabId());

                    // 2) Produce derived event (proof topic for M15)
                    kt.send("Cab-Location-Enriched", locationEvent.cabId(), locationEvent);

                } else {
                    // stale/replay: skip produce (for now)
                    log.info("SKIP STALE seq={} cab={} eventId={}",
                            locationEvent.seq(), locationEvent.cabId(), locationEvent.eventId());
                }

                //Runtime.getRuntime().halt(1);

                ensureLockStillOwned(lockLost);

                // 3) Commit the consumed offset inside the same transaction
                kt.sendOffsetsToTransaction(offsetMap(topic, partition, offset), consumerGroupMetadata);

                return true;
            });

//            String pKey = processedKey(locationEvent.eventId());
//
//            boolean firstTime = redisTemplate.opsForValue().setIfAbsent(pKey, "1", Duration.ofHours(24));
//
//            if (Boolean.FALSE.equals(firstTime)) {
//                kafkaTemplate.executeInTransaction(kt -> {
//                    kt.sendOffsetsToTransaction(offsetMap(topic, partition, offset), consumerGroupMetadata);
//                    return true;
//                });
//                log.info("SKIP+COMMIT(TX) instance={} partition={} offset={} eventId={}",
//                        instanceId, partition, offset, locationEvent.eventId());
//                return;
//            }

            log.info("PROCESSED+SENT+COMMIT(TX) instance={} partition={} offset={} eventId={} cab={} seq={}",
                    instanceId, partition, offset, locationEvent.eventId(), locationEvent.cabId(), locationEvent.seq());


        } catch (Exception e) {
            //Because now the seq-guard makes replays safe, and deleting it can reintroduce churn/retries.
//            redisTemplate.delete(processedKey);
//            ack.nack(Duration.ofMillis(2000));

            // classify only the lock-related ones
            if (e.getMessage() != null && e.getMessage().contains("Lost lock ownership")) {
                log.info("Lock lost mid-flight; retry later. eventId={} cab={} seq={}",
                        locationEvent.eventId(), locationEvent.cabId(), locationEvent.seq());
                ack.nack(Duration.ofMillis(2000));
                return;
            }
            throw e; // real failure -> let handler / DLT do its job
        } finally {
            try {

                try {
                    renewFuture.cancel(true);
                } catch (Exception e) {

                }
                try {
                    scheduler.shutdownNow();
                } catch (Exception e) {
                }

                if (lockAcquired) {
                    Long deleted = redisTemplate.execute(UNLOCK_SCRIPT, Collections.singletonList(lockKey), token);
                    if (deleted == null) log.warn("UNLOCK null cab={} eventId={}", locationEvent.cabId(), locationEvent.eventId());
                    else if (deleted == 1L) log.info("UNLOCK ok cab={} eventId={}", locationEvent.cabId(), locationEvent.eventId());
                    else log.info("UNLOCK no-op code={} cab={} eventId={}", deleted, locationEvent.cabId(), locationEvent.eventId());
                }

            } catch (Exception ignored) {
            }
        }
    }

    private void ensureLockStillOwned(AtomicBoolean lockLost) {
        if (lockLost.get()) throw new RuntimeException("Lost lock ownership");
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
