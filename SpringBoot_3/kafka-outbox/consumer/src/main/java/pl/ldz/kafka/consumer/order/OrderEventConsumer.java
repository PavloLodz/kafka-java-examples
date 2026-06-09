package pl.ldz.kafka.consumer.order;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import pl.ldz.kafka.common.dto.OrderEventDto;
import pl.ldz.kafka.consumer.idempotency.ProcessedMessage;
import pl.ldz.kafka.consumer.idempotency.ProcessedMessageRepository;

@Component
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);

    private final ProcessedMessageRepository processedMessageRepository;
    private final OrderEventHandler handler;

    public OrderEventConsumer(ProcessedMessageRepository processedMessageRepository,
                              OrderEventHandler handler) {
        this.processedMessageRepository = processedMessageRepository;
        this.handler = handler;
    }

    @KafkaListener(
            topics = "orders.events",
            groupId = "outbox-consumer-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    @Transactional
    public void onMessage(ConsumerRecord<String, OrderEventDto> record) {
        OrderEventDto event = record.value();

        if (processedMessageRepository.existsById(event.eventId())) {
            log.info("Duplicate event {}, skipping", event.eventId());
            return;
        }

        handler.handle(event);

        processedMessageRepository.save(new ProcessedMessage(event.eventId()));
        log.info("Processed event {}", event.eventId());
    }
}
