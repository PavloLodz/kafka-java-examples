package pl.ldz.kafka.consumer.dlq;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import pl.ldz.kafka.common.dto.OrderEventDto;

@Component
public class DlqPublisher {

  private static final Logger log = LoggerFactory.getLogger(DlqPublisher.class);
  private static final String DLQ_SUFFIX = ".DLQ";

  private final KafkaTemplate<String, OrderEventDto> kafkaTemplate;

  public DlqPublisher(KafkaTemplate<String, OrderEventDto> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  public void sendToDlq(ConsumerRecord<String, OrderEventDto> record, Exception cause) {
    String dlqTopic = record.topic() + DLQ_SUFFIX;
    log.error("Sending to DLQ topic={} key={} reason={}", dlqTopic, record.key(), cause.getMessage());

    kafkaTemplate.send(dlqTopic, record.key(), record.value()).whenComplete((result, ex) -> {
      if (ex != null) {
        log.error("Failed to publish to DLQ {}", dlqTopic, ex);
      }
    });
  }
}
