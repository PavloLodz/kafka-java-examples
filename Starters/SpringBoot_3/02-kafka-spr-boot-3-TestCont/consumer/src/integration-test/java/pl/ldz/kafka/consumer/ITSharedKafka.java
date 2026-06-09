package pl.ldz.kafka.consumer;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Shared Kafka container for all integration tests.
 *
 * <p>The container is started once in a static initializer and reused across every
 * test class that extends this base (Spring context caching keeps it alive for the
 * whole test run). Broker coordinates are injected via {@link DynamicPropertySource}
 * so no hard-coded port or {@code @ServiceConnection} magic is needed — the same
 * approach used in PavloLodz/Chat02 for PostgreSQL.</p>
 */
public abstract class ITSharedKafka {

    protected static final KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    static {
        kafka.start();
    }

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }
}
