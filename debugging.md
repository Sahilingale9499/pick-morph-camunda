# Debugging guide

## 1. Application (Spring Boot)
- Run tests:
  ```bash
  ./mvnw test
  ```
- Run locally (without Docker):
  ```bash
  ./mvnw spring-boot:run
  ```
- Check logs in the running terminal for stack traces.

## 2. Docker / Compose issues
- See running containers:
  ```bash
  docker ps
  ```
- See logs for a specific service (e.g. spring-camunda):
  ```bash
  docker compose logs spring-camunda
  ```
- Recreate stack from scratch:
  ```bash
  ./setup.sh
  ```

Common port issues:
- If a port (e.g. 5432 or 9191) is already in use, stop the process using it or change the host port mapping in `docker-compose.yml`.

## 3. Kafka-related issues
- Ensure Zookeeper and Kafka containers are healthy.
- Check Kafka logs:
  ```bash
  docker compose logs kafka
  ```
- Confirm topics and messages using your preferred Kafka CLI or UI tool.

## 5. General tips
- Use `docker compose logs -f <service>` to follow logs live.
- For persistent issues, clean local Docker state for this project:
  ```bash
  docker compose down -v
  ```
- When in doubt, rebuild the JAR first:
  ```bash
  ./mvnw clean package -DskipTests
  ```
