package pl.ldz.kafka.supplier.model;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record Order(
    String orderId,
    String customerId,
    String product,
    int quantity,
    BigDecimal price,
    LocalDateTime createdAt
) {
  public Order {
    if (createdAt == null) {
      createdAt = LocalDateTime.now();
    }
  }
}
