package pl.ldz.kafka.consumer.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import pl.ldz.kafka.consumer.model.Order;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class OrderConsumerService {

  private final CountDownLatch latch = new CountDownLatch(1);
  private final AtomicInteger receivedCount = new AtomicInteger(0);

  @KafkaListener(topics = "orders-topic", groupId = "consumer-group")
  public void consume(Order order) {
    System.out.println("Received order: " + order);
    receivedCount.incrementAndGet();
    latch.countDown();
  }

  public CountDownLatch getLatch() {
    return latch;
  }

  public int getReceivedCount() {
    return receivedCount.get();
  }
}
