package pl.ldz.kafka.producer;

import org.junit.jupiter.api.BeforeAll;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Shared Testcontainers base for all producer integration tests.
 *
 * <p>Containers are declared statically so Spring can cache the context across
 * subclasses, but they are only <em>started</em> inside a {@code @BeforeAll}
 * that first checks Docker availability via {@code assumeTrue}. When Docker is
 * absent the entire test class is skipped rather than failing with
 * {@code ExceptionInInitializerError}.</p>
 */
@SpringBootTest
@ActiveProfiles("it")
public abstract class ITSharedContainers {

  protected static final KafkaContainer kafka =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

  protected static final PostgreSQLContainer<?> postgres =
      new PostgreSQLContainer<>("postgres:16-alpine")
          .withDatabaseName("outbox")
          .withUsername("outbox")
          .withPassword("outbox");

  @BeforeAll
  static void startContainers() {
    assumeTrue(
        DockerClientFactory.instance().isDockerAvailable(),
        "Docker is not available in this environment — skipping"
    );
    if (!kafka.isRunning()) kafka.start();
    if (!postgres.isRunning()) postgres.start();
  }

  @DynamicPropertySource
  static void overrideProperties(DynamicPropertyRegistry registry) {
    registry.add("spring.kafka.bootstrap-servers", () ->
        kafka.isRunning() ? kafka.getBootstrapServers() : "localhost:9092");
    registry.add("spring.datasource.url", () ->
        postgres.isRunning() ? postgres.getJdbcUrl() : "jdbc:postgresql://localhost:5432/outbox");
    registry.add("spring.datasource.username", () ->
        postgres.isRunning() ? postgres.getUsername() : "outbox");
    registry.add("spring.datasource.password", () ->
        postgres.isRunning() ? postgres.getPassword() : "outbox");
  }
}
