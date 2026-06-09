package pl.ldz.kafka.supplier;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import pl.ldz.kafka.supplier.model.Order;
import pl.ldz.kafka.supplier.service.OrderSupplierService;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class OrderSupplierServiceTest {

  @Mock
  private KafkaTemplate<String, Order> kafkaTemplate;

  @InjectMocks
  private OrderSupplierService service;

  @Test
  void shouldSendOrder() {
    Order order = new Order("test-1", "cust-1", "Laptop", 1, null, null);

    service.sendOrder(order);

    verify(kafkaTemplate).send(eq("orders-topic"), eq("test-1"), eq(order));
  }
}
