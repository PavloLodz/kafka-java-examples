package pl.ldz.kafka.consumer;

import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Smoke test — verifies Docker is reachable. Skips gracefully when Docker is absent.
 */
public class DockerShouldWorkIT {

  @Test
  void checkDockerConnection() {
    assumeTrue(
        DockerClientFactory.instance().isDockerAvailable(),
        "Docker is not available in this environment — skipping"
    );
    var client = DockerClientFactory.instance().client();
    System.out.println("Docker API version: " + client.versionCmd().exec().getApiVersion());
    System.out.println("Docker is accessible!");
  }
}
