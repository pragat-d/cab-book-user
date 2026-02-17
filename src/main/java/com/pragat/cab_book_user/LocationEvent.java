package com.pragat.cab_book_user;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.sql.Timestamp;

@JsonIgnoreProperties(ignoreUnknown = true)
public record LocationEvent(
        String eventId,
        String cabId,
        double lat,
        double lon,
        Timestamp timestamp,
        Long seq
) {
}