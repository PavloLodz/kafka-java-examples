package pl.ldz.kafka.consumer.streams;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import pl.ldz.kafka.consumer.model.Order;

// TODO: This file realted to Kafka Sterams. I'll do it latter.

//@Configuration
public class OrderTopology {

  /*
  public KStream<String, Order> orderStream(StreamsBuilder streamsBuilder) {

    KStream<String, Order> stream = streamsBuilder.stream("orders-topic",
        Consumed.with(org.apache.kafka.common.serialization.Serdes.String(),
            new org.springframework.kafka.support.serializer.JsonSerde<>(Order.class)));
    

    stream.print(Printed.toSysOut());

    // Example: filter expensive orders
    stream.filter((key, order) -> order.price().doubleValue() > 500.0)
        .to("expensive-orders-topic");

    return stream;
  }
      */
}
