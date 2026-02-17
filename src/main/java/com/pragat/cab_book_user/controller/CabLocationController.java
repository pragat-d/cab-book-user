package com.pragat.cab_book_user.controller;

import com.pragat.cab_book_user.LocationEvent;
import com.pragat.cab_book_user.dto.CabLocationDto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoSearchCommandArgs;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@RestController
@RequestMapping("cabs")
public class CabLocationController {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;


    @GetMapping("/{cabId}/location")
    public ResponseEntity<CabLocationDto> getCabLocation(@PathVariable String cabId) {

        String key = "cab:loc:" + cabId;

        Map<Object, Object> map = stringRedisTemplate.opsForHash().entries(key);

        if(map == null || map.isEmpty())  return  ResponseEntity.notFound().build();
        return ResponseEntity.ok(new CabLocationDto( cabId,
                (String) map.get("lat"),
                (String) map.get("lon"),
                (String) map.get("ts"),
                (String) map.get("seq"),
                (String) map.get("eventId")));
    }

    @GetMapping("/nearby")
    public ResponseEntity<List<CabLocationDto>> getNearByCabs(
            @RequestParam double lat,
            @RequestParam double lon,
            @RequestParam(defaultValue = "5") double radiusKm,
            @RequestParam(defaultValue = "3") int limit
    ) {

        String geoKey = "cab:geo";

        Circle within = new Circle(new Point(lon,lat), new Distance(radiusKm, Metrics.KILOMETERS));

        RedisGeoCommands.GeoRadiusCommandArgs radiusArgs = RedisGeoCommands.GeoRadiusCommandArgs.newGeoRadiusArgs()
                .includeDistance()
                .sortAscending()
                .limit(limit);

        var results = stringRedisTemplate.opsForGeo().radius(geoKey,within,radiusArgs);

        if (results == null || results.getContent().isEmpty()) {
            return ResponseEntity.ok(List.of());
        }

        List<CabLocationDto> out = results.getContent().stream()
                .map(r -> (String) r.getContent().getName())
                .map(cabId -> {
                    Map<Object,Object> map = stringRedisTemplate.opsForHash().entries("cab:loc:"+cabId);
                    if(map == null || map.isEmpty()) return null;

                    return new CabLocationDto(
                            cabId,
                            (String) map.get("lat"),
                            (String) map.get("lon"),
                            (String) map.get("ts"),
                            (String)  map.get("seq"),
                            (String) map.get("eventId")
                    );
                })
                .filter(Objects::nonNull)
                .toList();

        return ResponseEntity.ok(out);


    }
}
