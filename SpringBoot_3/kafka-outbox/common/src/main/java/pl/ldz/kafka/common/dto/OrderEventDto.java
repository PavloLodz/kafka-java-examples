package pl.ldz.kafka.common.dto;

import java.util.UUID;

public record OrderEventDto(
    UUID eventId,
    UUID orderId,
    String status,
    String payload
) {
}
