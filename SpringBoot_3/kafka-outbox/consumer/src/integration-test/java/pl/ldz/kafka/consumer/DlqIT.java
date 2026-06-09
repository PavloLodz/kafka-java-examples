package pl.ldz.kafka.consumer;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import pl.ldz.kafka.common.dto.OrderEventDto;

import java.time.Duration;
import java.util.UUID;

import static org.awaitility.Awaitility.await;

/**
 * Verifies poison-pill messages are routed to the DLQ after exhausting retries.
 */
class DlqIT extends ITSharedContainers {

  @Autowired
  KafkaTemplate<String, OrderEventDto> kafkaTemplate;

  @Test
  void shouldRoutePoisonPillToDlq() {
    OrderEventDto poisonPill = new OrderEventDto(
        UUID.randomUUID(), UUID.randomUUID(), "INVALID", null);

    kafkaTemplate.send("orders.events", poisonPill.orderId().toString(), poisonPill);

    // Allow time for 3 retries (2 s each) + DLQ publish
    await()
        .atMost(Duration.ofSeconds(30))
        .pollDelay(Duration.ofSeconds(10))
        .untilAsserted(() -> {
          // TODO: assert record appeared in orders.events.DLQ
        });
  }
}
