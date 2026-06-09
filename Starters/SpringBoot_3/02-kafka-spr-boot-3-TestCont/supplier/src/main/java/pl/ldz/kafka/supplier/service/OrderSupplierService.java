package pl.ldz.kafka.supplier.service;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import pl.ldz.kafka.supplier.model.Order;

import java.math.BigDecimal;
import java.util.UUID;

@Service
public class OrderSupplierService {

  private final KafkaTemplate<String, Order> kafkaTemplate;

  public OrderSupplierService(KafkaTemplate<String, Order> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  public void sendOrder(Order order) {
    String key = order.orderId();
    kafkaTemplate.send("orders-topic", key, order);
    System.out.println("Sent order: " + order);
  }

  public Order createRandomOrder() {
    return new Order(
        UUID.randomUUID().toString(),
        "customer-" + (int) (Math.random() * 100),
        "Product-" + (int) (Math.random() * 50),
        (int) (Math.random() * 10) + 1,
        BigDecimal.valueOf(Math.random() * 1000).setScale(2, BigDecimal.ROUND_HALF_UP),
        null
    );
  }
}
