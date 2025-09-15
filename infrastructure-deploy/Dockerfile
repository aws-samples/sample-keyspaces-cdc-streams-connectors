# Multi-stage build for KCL Keyspaces application
FROM amazoncorretto:11-alpine AS builder

# Install Maven and build dependencies
RUN apk add --no-cache \
    maven \
    git \
    && rm -rf /var/cache/apk/*

# Set working directory
WORKDIR /app

# Copy pom files first for better layer caching
COPY  pom.xml ./
COPY  kcl-keyspaces-core/pom.xml ./kcl-keyspaces-core/
COPY  kcl-keyspaces-app/pom.xml ./kcl-keyspaces-app/
COPY  s3-bucket-connector/pom.xml ./s3-bucket-connector/
COPY  s3-vector-connector/pom.xml ./s3-vector-connector/
# Download dependencies (this layer will be cached if pom files don't change)
RUN mvn dependency:go-offline -B

# Copy source code
COPY kcl-keyspaces-core/src ./kcl-keyspaces-core/src
COPY kcl-keyspaces-app/src ./kcl-keyspaces-app/src
COPY s3-bucket-connector/src ./s3-bucket-connector/src
COPY s3-vector-connector/src ./s3-vector-connector/src

# Build the application with shade plugin to create fat JAR
RUN mvn clean package -DskipTests

# Runtime stage
FROM amazoncorretto:11-alpine

# Install necessary packages for AWS ECS
RUN apk add --no-cache \
    curl \
    ca-certificates \
    && rm -rf /var/cache/apk/*

# Create app user for security
RUN addgroup -g 1000 appuser && adduser -D -s /bin/sh -u 1000 -G appuser appuser

# Set working directory
WORKDIR /app

# Create lib directory for dependencies
RUN mkdir -p lib

# Copy the built JAR from builder stage
COPY --from=builder /app/kcl-keyspaces-app/target/kcl-keyspaces-app-1.0.0.jar ./app.jar
# Copy connector JARs to lib directory for runtime dependencies
COPY --from=builder /app/s3-bucket-connector/target/s3-bucket-connector-1.0.0.jar ./lib/
COPY --from=builder /app/s3-vector-connector/target/s3-vector-connector-1.0.0.jar ./lib/
COPY --from=builder /app/kcl-keyspaces-core/target/kcl-keyspaces-core-1.0.0.jar ./lib/

# Copy configuration files
COPY --from=builder /app/kcl-keyspaces-app/src/main/resources/streams-application.conf ./streams-application.conf
COPY --from=builder /app/kcl-keyspaces-app/src/main/resources/logback.xml ./logback.xml

ADD https://certs.secureserver.net/repository/sf-class2-root.crt ./sf-class2-root.crt

# Change ownership to app user
RUN chown -R appuser:appuser /app


# Switch to app user
USER appuser

# Set Java options for ECS deployment
ENV JAVA_OPTS="-Xms512m -Xmx2g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"

# Expose port (if needed for health checks)
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

ENV APPLICATION_CONF_LOCATION="/app/streams-application.conf"

# Set the entrypoint
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -cp \".:lib/*\" --add-opens java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED --add-opens java.base/sun.nio.ch=org.apache.arrow.memory.core,ALL-UNNAMED -jar app.jar"]
