package pl.ldz.kafka.consumer.integration;

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
 * Integration test for {@link OrderConsumerService}.
 *
 * <p>Extends {@link ITSharedKafka} — the container starts once statically and
 * {@code @DynamicPropertySource} wires {@code spring.kafka.bootstrap-servers}
 * automatically. No {@code @Import}, no {@code @ServiceConnection} needed.</p>
 */
@SpringBootTest
@ActiveProfiles("test")
class ConsumerIT extends ITSharedKafka {

    @Autowired
    private KafkaTemplate<String, Order> kafkaTemplate;

    @Autowired
    private OrderConsumerService consumerService;

    @Test
    void shouldConsumeMessage() {
        Order order = new Order("it-001", "cust-it", "Keyboard", 1, BigDecimal.valueOf(149.99), null);

        kafkaTemplate.send("orders-topic", order.orderId(), order);

        await()
            .atMost(Duration.ofSeconds(30))
            .untilAsserted(() ->
                assertThat(consumerService.getReceivedCount()).isGreaterThan(0)
            );
    }
}
