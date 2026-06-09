package pl.ldz.kafka.producer.order;

import jakarta.persistence.*;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "orders")
public class Order {

  @Id
  @GeneratedValue(strategy = GenerationType.UUID)
  private UUID id;

  @Column(nullable = false)
  private String itemName;

  @Column(nullable = false)
  private int quantity;

  @Column(nullable = false)
  private Instant createdAt = Instant.now();

  public Order() {
  }

  public Order(OrderRequest request) {
    this.itemName = request.itemName();
    this.quantity = request.quantity();
  }

  public UUID getId() {
    return id;
  }

  public String getItemName() {
    return itemName;
  }

  public int getQuantity() {
    return quantity;
  }

  public Instant getCreatedAt() {
    return createdAt;
  }
}
