package pl.ldz.kafka.producer;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import pl.ldz.kafka.producer.order.OrderRequest;
import pl.ldz.kafka.producer.order.OrderService;
import pl.ldz.kafka.producer.outbox.OutboxEvent;
import pl.ldz.kafka.producer.outbox.OutboxEventRepository;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies the full outbox relay cycle:
 * OrderService writes → OutboxEventRelay publishes to Kafka → status flips to SENT.
 */
class OutboxRelayIT extends ITSharedContainers {

  @Autowired
  OrderService orderService;
  @Autowired
  OutboxEventRepository outboxRepository;

  @Test
  void shouldRelayOutboxEventToKafka() throws Exception {
    orderService.createOrder(new OrderRequest("item-1", 2));

    await()
        .atMost(Duration.ofSeconds(15))
        .untilAsserted(() ->
            assertThat(outboxRepository.findAll())
                .anyMatch(e -> e.getStatus() == OutboxEvent.Status.SENT));
  }
}
