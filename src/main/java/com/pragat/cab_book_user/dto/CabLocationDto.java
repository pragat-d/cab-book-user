package com.pragat.cab_book_user.dto;

public record CabLocationDto(
        String cabId,
        String lat,
        String lon,
        String ts,
        String seq,
        String eventId
) {}

