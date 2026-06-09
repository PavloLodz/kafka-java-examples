package pl.ldz.kafka.consumer.integration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import pl.ldz.kafka.consumer.model.Order;
import pl.ldz.kafka.consumer.service.OrderConsumerService;

import java.math.BigDecimal;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = {"orders-topic"})
@DirtiesContext
class ConsumerIT {

  @Autowired
  private KafkaTemplate<String, Order> kafkaTemplate;

  @Autowired
  private OrderConsumerService consumerService;

  @Test
  void shouldConsumeMessage() {
    Order order = new Order("it-001", "cust-it", "Keyboard", 1, BigDecimal.valueOf(149.99), null);

    kafkaTemplate.send("orders-topic", order.orderId(), order);

    await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
        assertThat(consumerService.getReceivedCount()).isGreaterThan(0)
    );
  }
}
