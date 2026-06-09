package pl.ldz.kafka.consumer.e2e;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import pl.ldz.kafka.consumer.ITSharedKafka;
import pl.ldz.kafka.consumer.model.Order;
import pl.ldz.kafka.consumer.service.OrderConsumerService;

import java.math.BigDecimal;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end test: supplier sends an {@link Order}, consumer processes it.
 *
 * <p>Extends {@link ITSharedKafka} so the same single Kafka container is reused
 * across all IT classes — no extra Docker overhead per test class.</p>
 */
@SpringBootTest
@ActiveProfiles("test")
class SupplierConsumerE2ETest extends ITSharedKafka {

    @Autowired
    private KafkaTemplate<String, Order> kafkaTemplate;

    @Autowired
    private OrderConsumerService consumerService;

    @Test
    void shouldSendFromSupplierAndConsume() {
        Order order = new Order("e2e-123", "cust-e2e", "Monitor", 2, BigDecimal.valueOf(899.99), null);

        kafkaTemplate.send("orders-topic", order.orderId(), order);

        await()
            .atMost(Duration.ofSeconds(30))
            .untilAsserted(() ->
                assertThat(consumerService.getReceivedCount()).isGreaterThan(0)
            );
    }
}
