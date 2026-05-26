package pl.ldz.kafka.consumer.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonSerde;
import pl.ldz.kafka.consumer.model.Order;

import java.math.BigDecimal;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

// TODO: this part related with Kafka Streams. I'll see it latter.

@Disabled
class OrderTopologyTest {

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, Order> inputTopic;
  private TestOutputTopic<String, Order> outputTopic;

  @BeforeEach
  void setup() {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

    // 1. Create a StreamsBuilder
    StreamsBuilder builder = new StreamsBuilder();

    // 2. Call your topology method to register processors on the builder
    // new OrderTopology().orderStream(builder);

    // 3. Build the topology and pass it to the driver (FIXES THE NPE)
    testDriver = new TopologyTestDriver(builder.build(), props);

    // 4. Create test input/output topics with matching serializers/deserializers
    JsonSerde<Order> orderSerde = new JsonSerde<>(Order.class);
    inputTopic = testDriver.createInputTopic("orders-topic",
        Serdes.String().serializer(),
        orderSerde.serializer());

    outputTopic = testDriver.createOutputTopic("expensive-orders-topic",
        Serdes.String().deserializer(),
        orderSerde.deserializer());    // In practice: use StreamsBuilder to build and pass to TestDriver
  }

  @AfterEach
  void tearDown() {
    if (testDriver != null) {
      testDriver.close();
    }
  }

  @Test
  void shouldFilterExpensiveOrders() {
    // This is a simplified skeleton - full TopologyTestDriver implementation requires more setup
    assertThat(true).isTrue(); // Placeholder
  }
}
