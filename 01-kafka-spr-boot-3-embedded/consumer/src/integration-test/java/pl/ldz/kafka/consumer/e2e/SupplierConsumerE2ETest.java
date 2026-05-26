package pl.ldz.kafka.consumer.e2e;

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
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = {"orders-topic"})
@DirtiesContext
class SupplierConsumerE2ETest {

  @Autowired
  private KafkaTemplate<String, Order> kafkaTemplate;

  @Autowired
  private OrderConsumerService consumerService;

  @Test
  void shouldSendFromSupplierAndConsume() throws Exception {
    Order order = new Order("e2e-123", "cust-e2e", "Monitor", 2, BigDecimal.valueOf(899.99), null);

    kafkaTemplate.send("orders-topic", order.orderId(), order);

    boolean received = consumerService.getLatch().await(10, TimeUnit.SECONDS);
    assertThat(received).isTrue();
    assertThat(consumerService.getReceivedCount()).isGreaterThan(0);
  }
}
