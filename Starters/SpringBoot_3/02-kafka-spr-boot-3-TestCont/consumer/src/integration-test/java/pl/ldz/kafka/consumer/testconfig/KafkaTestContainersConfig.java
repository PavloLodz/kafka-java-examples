package pl.ldz.kafka.consumer.testconfig;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Shared TestContainers configuration that starts a single Kafka container
 * and wires its bootstrap-servers into the Spring context automatically via
 * {@link ServiceConnection}.
 *
 * <p>Import this class with {@code @Import(KafkaTestContainersConfig.class)}
 * in every integration-test that needs a real Kafka broker.</p>
 */
@TestConfiguration(proxyBeanMethods = false)
public class KafkaTestContainersConfig {

    @Bean
    @ServiceConnection
    public KafkaContainer kafkaContainer() {
        return new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));
    }
}
