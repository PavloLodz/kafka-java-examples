package pl.ldz.kafka.producer.order;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import pl.ldz.kafka.common.dto.OrderEventDto;
import pl.ldz.kafka.producer.outbox.OutboxEvent;
import pl.ldz.kafka.producer.outbox.OutboxEventRepository;

import java.util.UUID;

@Service
public class OrderService {

  private final OrderRepository orderRepository;
  private final OutboxEventRepository outboxRepository;
  private final ObjectMapper objectMapper;

  public OrderService(OrderRepository orderRepository,
                      OutboxEventRepository outboxRepository,
                      ObjectMapper objectMapper) {
    this.orderRepository = orderRepository;
    this.outboxRepository = outboxRepository;
    this.objectMapper = objectMapper;
  }

  @Transactional
  public Order createOrder(OrderRequest request) throws Exception {
    Order order = orderRepository.save(new Order(request));

    OrderEventDto event = new OrderEventDto(
        UUID.randomUUID(),
        order.getId(),
        "CREATED",
        objectMapper.writeValueAsString(order)
    );

    OutboxEvent outbox = new OutboxEvent();
    outbox.setTopic("orders.events");
    outbox.setPayload(objectMapper.writeValueAsString(event));
    outboxRepository.save(outbox);

    return order;
  }
}
