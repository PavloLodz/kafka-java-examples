package pl.ldz.kafka.producer.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import pl.ldz.kafka.common.dto.OrderEventDto;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Component
public class OutboxEventRelay {

  private static final Logger log = LoggerFactory.getLogger(OutboxEventRelay.class);
  private static final int MAX_RETRIES = 5;

  private final OutboxEventRepository outboxRepository;
  private final KafkaTemplate<String, OrderEventDto> kafkaTemplate;
  private final ObjectMapper objectMapper;
  private final OutboxEventRelay self;

  public OutboxEventRelay(OutboxEventRepository outboxRepository,
                          KafkaTemplate<String, OrderEventDto> kafkaTemplate,
                          ObjectMapper objectMapper,
                          @Lazy OutboxEventRelay self) {
    this.outboxRepository = outboxRepository;
    this.kafkaTemplate = kafkaTemplate;
    this.objectMapper = objectMapper;
    this.self = self;
  }

  @Scheduled(fixedDelay = 1000)
  public void relay() {
    List<OutboxEvent> pending = self.fetchPending();
    if (pending.isEmpty()) return;

    List<OutboxEvent> toUpdate = new ArrayList<>();

    for (OutboxEvent event : pending) {
      try {
        OrderEventDto dto = objectMapper.readValue(event.getPayload(), OrderEventDto.class);
        kafkaTemplate.send(event.getTopic(), dto.orderId().toString(), dto).get();
        event.setStatus(OutboxEvent.Status.SENT);
        event.setSentAt(Instant.now());
        log.info("Relayed outbox event {}", event.getId());
      } catch (Exception e) {
        event.setRetryCount(event.getRetryCount() + 1);
        if (event.getRetryCount() >= MAX_RETRIES) {
          event.setStatus(OutboxEvent.Status.FAILED);
          log.error("Outbox event {} permanently failed after {} retries",
              event.getId(), MAX_RETRIES);
        } else {
          log.warn("Relay failed for event {}, retry {}",
              event.getId(), event.getRetryCount());
        }
      }
      toUpdate.add(event);
    }

    self.persistUpdates(toUpdate);
  }

  @Transactional(readOnly = true)
  public List<OutboxEvent> fetchPending() {
    return outboxRepository.findTop50ByStatusOrderByCreatedAtAsc(OutboxEvent.Status.NEW);
  }

  @Transactional
  public void persistUpdates(List<OutboxEvent> events) {
    outboxRepository.saveAll(events);
  }
}
