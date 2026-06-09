package pl.ldz.kafka.consumer.testconfig;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Alternative wiring via {@link ServiceConnection} — import this with
 * {@code @Import(KafkaTestContainersConfig.class)} when you prefer
 * {@code @ServiceConnection} over {@link ITSharedContainers} inheritance.
 */
@TestConfiguration(proxyBeanMethods = false)
public class KafkaTestContainersConfig {

  @Bean
  @ServiceConnection
  public KafkaContainer kafkaContainer() {
    return new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));
  }
}
