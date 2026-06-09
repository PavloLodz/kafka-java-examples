package pl.ldz.kafka.consumer.order;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import pl.ldz.kafka.common.dto.OrderEventDto;

@Component
public class OrderEventHandler {

    private static final Logger log = LoggerFactory.getLogger(OrderEventHandler.class);

    public void handle(OrderEventDto event) {
        // TODO: implement your business logic here
        log.info("Handling order event: orderId={} status={}", event.orderId(), event.status());
    }
}
