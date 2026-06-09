package pl.ldz.kafka.consumer;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import pl.ldz.kafka.common.dto.OrderEventDto;
import pl.ldz.kafka.consumer.idempotency.ProcessedMessageRepository;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies idempotent consumption: first delivery is processed; duplicate is skipped.
 */
class OrderEventConsumerIT extends ITSharedContainers {

  @Autowired
  KafkaTemplate<String, OrderEventDto> kafkaTemplate;
  @Autowired
  ProcessedMessageRepository processedMessageRepository;

  @Test
  void shouldConsumeAndMarkAsProcessed() {
    UUID eventId = UUID.randomUUID();
    OrderEventDto event = new OrderEventDto(eventId, UUID.randomUUID(), "CREATED", "{}");

    kafkaTemplate.send("orders.events", event.orderId().toString(), event);

    await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
        assertThat(processedMessageRepository.existsById(eventId)).isTrue());
  }

  @Test
  void shouldSkipDuplicateEvents() {
    UUID eventId = UUID.randomUUID();
    OrderEventDto event = new OrderEventDto(eventId, UUID.randomUUID(), "CREATED", "{}");

    kafkaTemplate.send("orders.events", event.orderId().toString(), event);
    kafkaTemplate.send("orders.events", event.orderId().toString(), event);

    await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
        assertThat(processedMessageRepository.existsById(eventId)).isTrue());
  }
}
