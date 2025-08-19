# Keyspaces CDC Streams to S3 Architecture

![Architecture Diagram](keyspaces-cdc-s3-architecture-numbered.png)

## Architecture Components

### Data Sources (1)
**1. Applications & CQL Client**
- External applications and CQL clients that perform database operations
- These can be web applications, microservices, or direct CQL command-line clients
- Perform INSERT, UPDATE, DELETE operations on Keyspaces tables

### Amazon Keyspaces Layer (2)
**2. Amazon Keyspaces Cluster**
- Managed Apache Cassandra-compatible database service
- Provides serverless, scalable NoSQL database capabilities

**3. Keyspaces Table (CDC Enabled)**
- Specific table: `media.media_content`
- CDC (Change Data Capture) enabled with configuration:
  ```cql
  WITH cdc = true
  AND CUSTOM_PROPERTIES = {
    'cdc_specification': {
      'view_type': 'NEW_AND_OLD_IMAGES'
    }
  };
  ```
- Captures all data changes (INSERT, UPDATE, DELETE) as stream events

**4. CDC Stream (Keyspaces Stream ARN)**
- Generated automatically when CDC is enabled on a table
- Stream ARN format: `arn:aws:cassandra:region:account:/keyspace/KEYSPACE_NAME/table/TABLE_NAME/stream/STREAM_LABEL`
- Contains change events with before/after images of data modifications

### Container Infrastructure (5)
**5. Container Infrastructure Cluster**
- Complete containerization setup for the streaming application

**6. ECR Repository**
- Amazon Elastic Container Registry
- Stores Docker images: `keyspaces-s3-streamer:latest`
- Built from `infrastructure/docker/Dockerfile` with Java 17 runtime

**7. ECS Fargate Cluster**
- Cluster name: `keyspaces-streamer-cluster`
- Serverless container orchestration
- Manages multiple tasks without server management

**8. ECS Tasks (Per Stream)**
- Container grouping for stream-specific processing
- Each Keyspaces stream ARN gets its own dedicated task

**9. & 10. Java Applications (ECS Tasks)**
- Containerized Java applications running the streaming logic
- Each task processes one specific Keyspaces stream ARN
- Contains three key components:
  - **Keyspaces Streams Kinesis Adapter**: Your custom library that bridges Keyspaces CDC to KCL
  - **Kinesis Client Library (KCL)**: Handles stream processing, checkpointing, and scaling
  - **S3RecordProcessor**: Custom processor that writes CDC events to S3
- Configuration:
  - CPU: 1024 (1 vCPU)
  - Memory: 2048 MB
  - Java heap: `-Xmx1536m -Xms512m`

### Storage & State Management (11)
**11. Storage & State Cluster**
- Persistent storage layer for data and application state

**12. S3 Data Bucket**
- Bucket name pattern: `keyspaces-data-{random-suffix}`
- Stores processed CDC events in JSON format
- Directory structure: `KEYSPACE/TABLE_NAME/YYYY-MM-DD/SEQUENCE_NUMBER.json`
- Example path: `media/media_content/2024-07-31/12345.json`

**13. DynamoDB Tables (Checkpoints)**
- Table name pattern: `keyspaces-streamer-{keyspace}-{table}`
- Example: `keyspaces-streamer-media-media_content`
- Stores KCL checkpoints to track stream processing progress
- Enables resuming from last processed position after restarts
- One table per Keyspaces stream ARN

### Monitoring & Observability (14)
**14. Monitoring Cluster**
- Complete observability setup for the streaming pipeline

**15. CloudWatch Logs**
- Log group: `/ecs/keyspaces-streamer`
- Stream prefix: `ecs-{stream-hash}`
- Retention: 7 days
- Contains application logs, errors, and processing information

**16. CloudWatch Metrics**
- Custom metrics from KCL and application
- Includes stream processing rates, errors, and performance metrics
- Used for monitoring and alerting

### Security & Permissions (17)
**17. ECS Task Role**
- IAM role: `keyspaces-streamer-task-role`
- Permissions:
  - **Keyspaces**: `cassandra:Select`, `cassandra:DescribeStream`, `cassandra:GetRecords`, etc.
  - **S3**: `s3:PutObject`, `s3:GetObject`
  - **DynamoDB**: Full access for checkpoint management
  - **CloudWatch**: `cloudwatch:PutMetricData`

## Data Flow Explanation

### Primary Data Flow (A-C)
- **A. INSERT/UPDATE/DELETE**: Applications perform database operations
- **B. CQL Operations**: Direct CQL commands from clients
- **C. CDC Events**: Keyspaces generates change events to the CDC stream

### Container Deployment Flow (D-E)
- **D. Pull Image**: ECS pulls container images from ECR
- **E. Run Tasks**: ECS cluster runs containerized applications

### Stream Processing Flow (F-G)
- **F. & G. Stream Records**: CDC events flow to Java applications via the Keyspaces Streams Kinesis Adapter
- The adapter translates Keyspaces CDC events into Kinesis-compatible format for KCL processing

### Data Persistence Flow (H-I)
- **H. & I. Store CDC Events**: Processed events are written to S3 in structured JSON format
- Each event contains the full change record with metadata

### State Management Flow (J-K)
- **J. & K. Save/Load Checkpoints**: Applications maintain processing state in DynamoDB
- Enables fault tolerance and exactly-once processing semantics

### Monitoring Flow (L-M)
- **L. Application Logs**: Detailed logging to CloudWatch Logs
- **M. Metrics**: Performance and operational metrics to CloudWatch

### Security Flow (N)
- **N. Permissions**: IAM role provides necessary permissions to all AWS services

## Key Features

### Scalability
- **Multi-stream Support**: Each Keyspaces stream ARN gets dedicated ECS task
- **Auto-scaling**: ECS Fargate automatically handles resource scaling
- **Parallel Processing**: Multiple streams processed simultaneously

### Reliability
- **Persistent Checkpoints**: DynamoDB ensures no data loss during restarts
- **Fault Tolerance**: KCL handles shard rebalancing and failure recovery
- **Retry Logic**: Built-in retry mechanisms for transient failures

### Monitoring
- **Comprehensive Logging**: All application events logged to CloudWatch
- **Metrics Collection**: Performance metrics for monitoring and alerting
- **Operational Visibility**: Complete observability into the streaming pipeline

### Security
- **IAM-based Access**: Fine-grained permissions for each service
- **VPC Integration**: Secure network configuration
- **Credential Management**: AWS credential provider chain for secure access

## Deployment Architecture

The system supports both automated and manual deployment:

1. **Automated Deployment**: `./infrastructure/scripts/deploy.sh`
2. **Manual Deployment**: Terraform-based infrastructure as code
3. **Configuration Management**: Auto-detection of AWS account and region
4. **Multi-environment Support**: Configurable via `terraform.tfvars`

This architecture provides a robust, scalable, and maintainable solution for streaming Amazon Keyspaces CDC events to S3 storage.
