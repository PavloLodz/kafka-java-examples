# Java Kafka Learning Project

## Structure
- `supplier` - Produces orders to Kafka
- `consumer` - Consumes orders + planned Kafka Streams example

## How to run

1. Start Kafka + Kafbat:
   ```bash
   docker compose up -d
   ```

2. Run supplier:
   ```bash
   mvn spring-boot:run -pl supplier -am
   ```

3. Run consumer:
   ```bash
   mvn spring-boot:run -pl consumer -am
   ```

4. Run tests:
   ```bash
   mvn clean verify
   ```

Access Kafbat UI at http://localhost:8080
