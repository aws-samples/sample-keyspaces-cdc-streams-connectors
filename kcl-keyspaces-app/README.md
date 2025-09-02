# KCL Keyspaces Application Module

This module provides a complete, runnable application for processing Amazon Keyspaces CDC (Change Data Capture) streams using the Kinesis Client Library (KCL). It serves as the main entry point and example implementation of the Keyspaces CDC Streams Connectors framework.

## Overview

The `kcl-keyspaces-app` module is a production-ready application that:

- **Processes CDC Streams**: Consumes change data capture events from Amazon Keyspaces tables
- **Integrates Connectors**: Supports both S3 Bucket and S3 Vector connectors out of the box
- **Provides Health Monitoring**: Includes a built-in health check endpoint
- **Handles Graceful Shutdown**: Implements proper shutdown hooks for production deployment
- **Supports Multiple Workers**: Can be scaled horizontally across multiple instances

## Architecture

The application follows a modular architecture:

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Amazon       │    │   KCL            │    │   Target       │
│   Keyspaces    │───▶│   Application    │───▶│   Connectors   │
│   CDC Stream   │    │   (This Module)  │    │   (S3, etc.)   │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## Features

- **KCL Integration**: Built on Amazon Kinesis Client Library for reliable stream processing
- **Connector Framework**: Pluggable connector architecture for different target systems
- **Health Monitoring**: HTTP health check endpoint on port 8080
- **Graceful Shutdown**: Proper cleanup on SIGTERM/SIGINT signals
- **Worker Management**: Automatic worker ID generation and management
- **Configuration Management**: Flexible configuration via `stream-application.conf` and environment variables
- **Logging**: Comprehensive logging with SLF4J and Logback

## Prerequisites

- Java 11 or higher
- Maven 3.6 or higher
- AWS credentials configured (IAM role, credentials file, or environment variables)
- Amazon Keyspaces table with CDC enabled
- Appropriate AWS permissions for the target connectors

## Quick Start

### 1. Build the Application

```bash
mvn clean package
```

This creates a shaded JAR with all dependencies included.

### 2. Configure the Application

Edit `src/main/resources/application.conf` with your settings:

```hocon
keyspaces-cdc-streams {
    stream {
        keyspace-name = "your-keyspace"
        table-name = "your-table"
        region = "us-east-1"
        application-name = "your-app-name"
    }
    
    connector {
        target-mapper = "software.amazon.ssa.streams.connector.s3.S3TargetMapper"
        bucket-id = "your-s3-bucket"
        prefix = "export/keyspace/table"
        region = "us-east-1"
        format = "avro"
        max-retries = 3
    }
}
```

### 3. Run the Application

```bash
# Using Maven
mvn exec:java

# Using the JAR directly
java -jar target/kcl-keyspaces-app-1.0.0.jar

# With custom configuration
java -jar target/kcl-keyspaces-app-1.0.0.jar --config-path=/path/to/config.conf
```

## Configuration

### Stream Configuration

| Parameter | Required | Description |
|-----------|----------|-------------|
| `keyspace-name` | Yes | Amazon Keyspaces keyspace name |
| `table-name` | Yes | Table name with CDC enabled |
| `region` | Yes | AWS region for Keyspaces |
| `application-name` | Yes | Unique name for the KCL application |
| `stream-name` | No | Specific stream name (optional) |
| `stream-arn` | No | Specific stream ARN (optional) |

### Connector Configuration

Choose one of the available connectors:

#### S3 Bucket Connector
```hocon
connector {
    target-mapper = "software.amazon.ssa.streams.connector.s3.S3TargetMapper"
    bucket-id = "your-bucket"
    prefix = "export/keyspace/table"
    region = "us-east-1"
    format = "avro"  # or "json"
    timestamp-partition = "hours"
    max-retries = 3
}
```

#### S3 Vector Connector
```hocon
connector {
    target-mapper = "software.amazon.ssa.streams.connector.s3.S3VectorTargetMapper"
    bucket-id = "your-vector-bucket"
    region = "us-east-1"
    index-name = "your-vector-index"
    embedding-field = "description"
    key-field = "id"
    metadata-fields = ["title", "category"]
    dimensions = 256
    max-retries = 3
}
```

### KCL Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `skip-shard-sync-at-worker-initialization-if-leases-exist` | Skip shard sync if leases exist | `true` |
| `parent-shard-poll-interval-millis` | Parent shard polling interval | `1000` |
| `shard-consumer-dispatch-poll-interval-millis` | Consumer dispatch polling interval | `500` |

### Lease Management

| Parameter | Description | Default |
|-----------|-------------|---------|
| `shard-sync-interval-millis` | Shard synchronization interval | `60000` |
| `leases-recovery-auditor-inconsistency-confidence-threshold` | Recovery auditor threshold | `3` |
| `leases-recovery-auditor-execution-frequency-millis` | Recovery auditor frequency | `5000` |
| `lease-assignment-interval-millis` | Lease assignment interval | `1000` |

## Application Components

### KCLMainRunner

The main application entry point that:

- Initializes the Keyspaces configuration
- Creates and starts the KCL scheduler
- Sets up graceful shutdown hooks
- Starts the health check server
- Manages the worker lifecycle

### HealthServer

A simple HTTP server that provides:

- Health check endpoint at `/health`
- Returns HTTP 200 OK when the application is running
- Useful for load balancers and monitoring systems

### KCLScheduler

Manages the KCL scheduler lifecycle:

- Creates the scheduler with proper configuration
- Handles worker initialization and cleanup
- Manages graceful shutdown procedures

## Deployment

### Docker Deployment

The application can be deployed using Docker:

```dockerfile
FROM openjdk:11-jre-slim
COPY target/kcl-keyspaces-app-1.0.0.jar /app/app.jar
WORKDIR /app
CMD ["java", "-jar", "app.jar"]
```

### AWS ECS/Fargate

Deploy as a containerized service:

```yaml
# task-definition.json
{
  "family": "keyspaces-cdc-app",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "256",
  "memory": "512",
  "executionRoleArn": "arn:aws:iam::account:role/ecsTaskExecutionRole",
  "taskRoleArn": "arn:aws:iam::account:role/keyspaces-cdc-task-role",
  "containerDefinitions": [
    {
      "name": "keyspaces-cdc",
      "image": "your-registry/keyspaces-cdc:latest",
      "portMappings": [
        {
          "containerPort": 8080,
          "protocol": "tcp"
        }
      ]
    }
  ]
}
```

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: keyspaces-cdc-app
spec:
  replicas: 3
  selector:
    matchLabels:
      app: keyspaces-cdc
  template:
    metadata:
      labels:
        app: keyspaces-cdc
    spec:
      containers:
      - name: keyspaces-cdc
        image: your-registry/keyspaces-cdc:latest
        ports:
        - containerPort: 8080
        env:
        - name: AWS_REGION
          value: "us-east-1"
```

## Monitoring and Health Checks

### Health Endpoint

The application exposes a health check endpoint:

```bash
curl http://localhost:8080/health
# Returns: OK
```

### Logging

Monitor the application through logs:

```bash
# View application logs
tail -f logs/kcl-application.log

# View specific connector logs
grep "S3TargetMapper" logs/kcl-application.log
```

### Metrics

Key metrics to monitor:

- **Record Processing Rate**: Records processed per second
- **Error Rate**: Failed record processing attempts
- **Latency**: Time from record receipt to processing completion
- **Worker Health**: Worker initialization and shutdown events

## Troubleshooting

### Common Issues

1. **Configuration Errors**
   - Verify all required parameters are set
   - Check AWS credentials and permissions
   - Validate connector-specific configuration

2. **Connection Issues**
   - Verify network connectivity to Keyspaces
   - Check VPC and security group configuration
   - Validate SSL certificate settings

3. **Performance Issues**
   - Monitor worker count and shard distribution
   - Check connector-specific performance metrics
   - Review KCL configuration parameters

### Debug Mode

Enable debug logging:

```hocon
# In logback.xml
<logger name="software.amazon.ssa.streams" level="DEBUG" />
<logger name="software.amazon.ssa.streams.connector" level="DEBUG" />
```

### Environment Variables

Override configuration with environment variables:

```bash
export KEYSPACES_CDC_STREAMS_STREAM_KEYSPACE_NAME="your-keyspace"
export KEYSPACES_CDC_STREAMS_STREAM_TABLE_NAME="your-table"
export KEYSPACES_CDC_STREAMS_STREAM_REGION="us-east-1"
```

## Scaling

### Horizontal Scaling

Scale the application by running multiple instances:

1. **Multiple Workers**: Each instance processes different shards
2. **Load Balancing**: Use load balancer for health checks
3. **Auto-scaling**: Configure ECS/Kubernetes auto-scaling policies

### Vertical Scaling

Optimize single instance performance:

1. **Memory**: Increase heap size for large record batches
2. **CPU**: Allocate more CPU for processing-intensive connectors
3. **Network**: Optimize network buffer sizes

## Security

### IAM Permissions

Ensure proper IAM roles and policies:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "keyspaces:GetTable",
        "keyspaces:DescribeTable"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "kinesis:DescribeStream",
        "kinesis:GetShardIterator",
        "kinesis:GetRecords"
      ],
      "Resource": "*"
    }
  ]
}
```

### Network Security

- Use VPC endpoints for AWS services
- Configure security groups appropriately
- Enable SSL/TLS for all connections

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

This project is licensed under the Apache License 2.0.
