# Runtime stage only — JAR is pre-built locally via ./mvnw clean package -DskipTests
FROM eclipse-temurin:17-jre-jammy
WORKDIR /app

# Add non-root user for security
RUN groupadd -r appgroup && useradd -r -g appgroup appuser

# Install curl for health checks
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

# Copy the pre-built jar
COPY target/*.jar app.jar

# Set ownership
RUN chown -R appuser:appgroup /app
USER appuser

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=60s --retries=3 \
  CMD curl -f http://localhost:9191/actuator/health || exit 1

# Expose port
EXPOSE 9191

# JVM options for containerized environment
ENV JAVA_OPTS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0 -XX:InitialRAMPercentage=50.0"

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
