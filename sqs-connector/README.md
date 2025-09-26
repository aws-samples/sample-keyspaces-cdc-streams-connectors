# Amazon SQS Connector for Keyspaces CDC Streams

The Amazon SQS Connector is a specialized component for the KCL Keyspaces CDC Streams framework that enables real-time message publishing to Amazon SQS queues. This connector processes Change Data Capture (CDC) events from Amazon Keyspaces tables and publishes them as JSON messages to SQS queues for downstream processing.

## Overview

This connector provides a reliable way to stream Keyspaces CDC events to SQS queues, enabling event-driven architectures and microservices communication. It supports multiple message formats, configurable retry logic, and comprehensive error handling for production environments.

## Features

- **Real-time Message Publishing**: Publishes CDC events to SQS queues as they occur
- **Multiple Message Formats**: Support for full, new-image, old-image, and fields-only formats
- **Configurable Retry Logic**: Built-in exponential backoff retry mechanism for failed operations
- **AWS Integration**: Native integration with Amazon SQS using AWS SDK v2
- **JSON Message Format**: Structured JSON messages with optional metadata
- **Message Delay Support**: Configurable message delay for delayed processing
- **Comprehensive Logging**: Detailed logging for monitoring and debugging
- **Batching Support**: Groups multiple records into single messages (up to 200KB) and sends in batches of 10

## Architecture

```
Source Table (CDC Enabled)
         ↓
   CDC Stream Events
         ↓
SQSTargetMapper
         ↓
Amazon SQS Queue
         ↓
Downstream Consumers
```

## Configuration

### Application Configuration

Add the following configuration to your `streams-application.conf`:

```hocon
keyspaces-cdc-streams {
    stream {
        keyspace-name = "your_keyspace"
        table-name = "source_table"
        region = "us-east-1"
        application-name = "sqs-stream-processor"
    }
    
    connector {
        target-mapper = "software.amazon.ssa.streams.connector.sqs.SQSTargetMapper"
        queue-url = "https://sqs.us-east-1.amazonaws.com/123456789012/your-queue"
        region = "us-east-1"
        message-format = "full"
        include-metadata = true
        fields-to-include = ['key', 'value']
        delay-seconds = 0
    }
    
    coordinator {
        skip-shard-sync-at-worker-initialization-if-leases-exist = trueL
        parent-shard-poll-interval-millis = 1000
        shard-consumer-dispatch-poll-interval-millis = 500
    }
    
    lease-management {
        shard-sync-interval-millis = 60000
        leases-recovery-auditor-inconsistency-confidence-threshold = 3
        leases-recovery-auditor-execution-frequency-millis = 5000
        lease-assignment-interval-millis = 1000
    }
    
    processor {
        call-process-records-even-for-empty-record-list = true
    }
}
```

## Configuration Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `queue-url` | String | Yes | - | SQS queue URL (e.g., https://sqs.region.amazonaws.com/account/queue-name) |
| `region` | String | No | `us-east-1` | AWS region for SQS operations |
| `message-format` | String | No | `full` | Message format: full, new-image, old-image, fields-only |
| `fields-to-include` | List[String] | No | `[]` | Fields to include (used with fields-only format) |
| `include-metadata` | Boolean | No | `true` | Include CDC metadata in messages |
| `delay-seconds` | Integer | No | `0` | Message delay in seconds (0-900) |

## Message Formats

### Full Format (Default)
```json
{
  "records": [
    {
      "metadata": {
        "keyspace" : "media",
        "table" : "movies",
        "operation": "INSERT",
        "sequenceNumber": "1234567890"
      },
      "newImage": {
        "title": "The Matrix",
        "overview": "A computer hacker learns about the true nature of reality...",
        "rel_date": "1999-03-31",
        "popularity": 85.2,
        "vote_count": 15000,
        "vote_average": 8.7
      },
      "oldImage": {
        "title": "The Matrix",
        "overview": "Previous overview...",
        "rel_date": "1999-03-31",
        "popularity": 80.0,
        "vote_count": 12000,
        "vote_average": 8.5
      }
    }
  ]
}


## Usage Examples

### 1. Create Source Table with CDC

```cql
CREATE TABLE IF NOT EXISTS media.movies (
    title text,
    overview text,
    original_lang text,
    rel_date date,
    popularity decimal,
    vote_count int,
    vote_average decimal,
    PRIMARY KEY (title)
) WITH cdc = true
AND CUSTOM_PROPERTIES = {
  'cdc_specification': {'view_type':'NEW_AND_OLD_IMAGES'}
};
```

### 2. Create SQS Queue

```bash
aws sqs create-queue --queue-name movies-cdc-events --region us-east-1
```

### 3. Configure Connector

```hocon
connector {
    target-mapper = "software.amazon.ssa.streams.connector.sqs.SQSTargetMapper"
    queue-url = "https://sqs.us-east-1.amazonaws.com/123456789012/movies-cdc-events"
    region = "us-east-1"
    message-format = "new-image"
    include-metadata = true
    delay-seconds = 0
}
```

### 4. Fields-Only Configuration

```hocon
connector {
    target-mapper = "software.amazon.ssa.streams.connector.sqs.SQSTargetMapper"
    queue-url = "https://sqs.us-east-1.amazonaws.com/123456789012/movies-cdc-events"
    message-format = "fields-only"
    fields-to-include = ["title", "rel_date", "popularity", "vote_average"]
    include-metadata = true
    max-retries = 3
}
```

## How It Works

1. **CDC Event Processing**: The connector receives CDC events from the source table
2. **Record Batching**: Groups multiple records into single messages (up to 200KB)
3. **Operation Detection**: Determines the operation type (INSERT, UPDATE, DELETE, TTL, etc.)
4. **Message Creation**: Creates JSON message based on configured format
5. **Metadata Addition**: Adds CDC metadata if enabled
6. **SQS Batch Publishing**: Sends up to 10 messages per batch with retry logic
7. **Error Handling**: Logs errors and continues processing other records

### Supported Operations

| Operation Type | Description | Message Content |
|----------------|-------------|-----------------|
| `INSERT` | New record inserted | newImage data |
| `UPDATE` | Record updated | newImage and oldImage data |
| `DELETE` | Record deleted | oldImage data |
| `TTL` | Record expired | oldImage data |
| `REPLICATED_*` | Replicated operations | Same as above |

## Batching Strategy

The connector implements a two-level batching strategy:

1. **Message Batching**: Groups multiple CDC records into a single SQS message (up to 200KB)
2. **SQS Batching**: Sends up to 10 messages per SQS batch request

This approach maximizes throughput while staying within SQS limits and message size constraints.

## Error Handling

The connector includes robust error handling:

- **Retry Logic**: Exponential backoff with configurable maximum retries
- **Error Logging**: Detailed logging of failed operations
- **Graceful Degradation**: Continues processing other records when individual operations fail
- **SQS Exception Handling**: Specific handling for SQS service errors

### Retryable Errors

- ThrottlingException
- ServiceUnavailableException
- InternalServerError
- RequestTimeoutException
- TooManyRequestsException
- HTTP 502, 503, 504 errors

## Performance Considerations

- **Message Size**: SQS has a 256KB message size limit (connector uses 200KB for safety)
- **Throughput**: SQS supports up to 300 messages per second per queue
- **Batch Processing**: Uses SQS batch operations for higher throughput
- **Connection Pooling**: AWS SDK handles connection pooling automatically

## Monitoring and Logging

The connector provides comprehensive logging:

- **INFO Level**: Successful operations and initialization
- **WARN Level**: Retry attempts and recoverable errors
- **ERROR Level**: Failed operations and configuration issues
- **DEBUG Level**: Detailed operation information

### CloudWatch Metrics

Monitor the following SQS metrics:

- `NumberOfMessagesSent`: Number of messages sent to the queue
- `NumberOfMessagesReceived`: Number of messages received from the queue
- `ApproximateAgeOfOldestMessage`: Age of oldest message in queue
- `ApproximateNumberOfMessagesVisible`: Number of visible messages

## Prerequisites

- Amazon Keyspaces table with CDC enabled
- SQS queue created in the target region
- AWS credentials configured (IAM role, credentials file, or environment variables)
- Java 11 or higher
- Maven 3.6 or higher

## Dependencies

- `kcl-keyspaces-core`: Core KCL Keyspaces functionality
- `aws-sdk-sqs`: AWS SDK for SQS operations
- `jackson-databind`: JSON processing
- `slf4j-api` and `logback-classic`: Logging framework

## Building

```bash
mvn clean package
```

This creates a fat JAR with all dependencies included.

## Deployment

1. **ECS/Fargate**: Deploy as a containerized application
2. **EC2**: Run as a standalone Java application
3. **Local Development**: Run with Maven exec plugin

## Troubleshooting

### Common Issues

1. **Queue Not Found**: Verify the queue URL is correct and the queue exists
2. **Permission Errors**: Ensure IAM role has SQS permissions (sqs:SendMessage)
3. **Message Size Exceeded**: Reduce message size or use fields-only format
4. **Throttling**: Increase retry attempts or implement exponential backoff

### Debug Mode

Enable debug logging by setting the log level:

```hocon
logger.sqs-connector = DEBUG
```

## Best Practices

1. **Message Format**: Choose the appropriate format for your use case
2. **Field Selection**: Use fields-only format to reduce message size
3. **Retry Configuration**: Adjust retry settings based on your latency requirements
4. **Monitoring**: Set up CloudWatch alarms for queue depth and error rates
5. **Testing**: Test with small datasets before production deployment
6. **Dead Letter Queues**: Configure DLQ for failed message processing

## Advanced Configuration

### Multiple Queues

You can configure multiple SQS connectors for different use cases:

```hocon
# Instance 1: Full events
connector {
    target-mapper = "software.amazon.ssa.streams.connector.sqs.SQSTargetMapper"
    queue-url = "https://sqs.us-east-1.amazonaws.com/123456789012/full-events"
    message-format = "full"
}

# Instance 2: Lightweight events
connector {
    target-mapper = "software.amazon.ssa.streams.connector.sqs.SQSTargetMapper"
    queue-url = "https://sqs.us-east-1.amazonaws.com/123456789012/lightweight-events"
    message-format = "fields-only"
    fields-to-include = ["title", "rel_date"]
}
```

### Dead Letter Queue Configuration

```bash
# Create DLQ
aws sqs create-queue --queue-name movies-cdc-events-dlq --region us-east-1

# Configure main queue with DLQ
aws sqs set-queue-attributes \
    --queue-url https://sqs.us-east-1.amazonaws.com/123456789012/movies-cdc-events \
    --attributes '{
        "RedrivePolicy": "{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:123456789012:movies-cdc-events-dlq\",\"maxReceiveCount\":3}"
    }'
```

## JEXL Filter Expressions

The SQS connector supports JEXL (Java Expression Language) filter expressions to selectively process records based on their content. This allows you to filter out unwanted records before they are sent to SQS, reducing costs and improving performance.

### Configuration

Add the `jexl-filter-expression` property to your configuration:

```hocon
keyspaces-cdc-streams {
  connector {
    jexl-filter-expression = "metadata.operation == 'INSERT'"
  }
}
```

### Available Data Context

JEXL expressions have access to the following data structure:

```javascript
{
  // Record metadata
  "metadata": {
    "sequenceNumber": "12345",
    "operation": "INSERT", // INSERT, UPDATE, DELETE, etc.
    "streamArn": null,
    "shardId": null,
    "approximateArrivalTimestamp": null
  },
  
  // New image data (for INSERT/UPDATE operations)
  "newImage": {
    "id": "user123",
    "name": "John Doe",
    "email": "john@example.com",
    "popularity": 150,
    "vote_average": 7.5
  },
  
  // Old image data (for UPDATE/DELETE operations)
  "oldImage": {
    "id": "user123",
    "name": "John Smith",
    "email": "john.smith@example.com"
  },
  
  // Flattened data (newImage takes precedence over oldImage)
  "id": "user123",
  "name": "John Doe",
  "email": "john@example.com",
  "popularity": 150,
  "vote_average": 7.5
}
```

### JEXL Expression Examples

#### Filter by Operation Type
```javascript
// Only process INSERT operations
metadata.operation == 'INSERT'

// Process INSERT and UPDATE operations
metadata.operation == 'INSERT' || metadata.operation == 'UPDATE'

// Exclude DELETE operations
metadata.operation != 'DELETE'
```

#### Filter by Field Values
```javascript
// Filter by numeric values
popularity > 100
vote_average >= 7.0
vote_count < 1000

// Filter by string values
original_lang == 'en'
title != null && title != ''

// Filter by boolean values
is_active == true
```

#### Complex Filtering
```javascript
// Multiple conditions with AND
metadata.operation == 'INSERT' && popularity > 50

// Multiple conditions with OR
popularity > 100 || vote_average > 8.0

// Complex nested conditions
(metadata.operation == 'INSERT' || metadata.operation == 'UPDATE') && 
(popularity > 50 || vote_average > 7.0) && 
original_lang == 'en'
```

#### Field Existence Checks
```javascript
// Check if field exists and has value
title != null && title != ''

// Check if numeric field is within range
popularity != null && popularity >= 10 && popularity <= 1000

// Check if date field is valid
rel_date != null && rel_date != '' && rel_date != 'NaN'
```

#### Nested Data Access
```javascript
// Access nested image data
newImage.popularity > 100
oldImage.vote_average < 5.0

// Compare old vs new values
newImage.popularity > oldImage.popularity
```

### Environment Variable Override

You can override the JEXL filter expression using environment variables:

```bash
export CONNECTOR_JEXL_FILTER_EXPRESSION="metadata.operation == 'INSERT'"
```

### Performance Considerations

- **Filter Early**: JEXL filtering happens before SQS processing, reducing costs
- **Simple Expressions**: Keep expressions simple for better performance
- **Index Usage**: Consider filtering on indexed fields for better performance
- **Logging**: Filter results are logged for monitoring

### Error Handling

- **Invalid Expressions**: Invalid JEXL expressions will cause startup failure
- **Evaluation Errors**: Records causing evaluation errors are filtered out
- **Missing Fields**: Accessing non-existent fields returns null
- **Type Mismatches**: JEXL handles type coercion automatically

### Sample Configuration

See `src/main/resources/sqs-application-with-jexl.conf` for complete examples.

## Code Quality

This project includes SpotBugs (FindBugs) static analysis for code quality assurance. The analysis is **enabled and working** with Java 11.

### SpotBugs Configuration

The SpotBugs plugin is configured in the `pom.xml` with the following settings:
- **Effort**: Maximum analysis effort
- **Threshold**: Low (reports all issues)
- **Output**: XML and HTML reports
- **Test Analysis**: Includes test code analysis
- **Exclusion File**: `spotbugs-exclude.xml` for suppressing false positives
- **Status**: ✅ **Active and working**

### Running SpotBugs Analysis

To run SpotBugs analysis:

1. Ensure you're using Java 11-21 (not Java 23)
2. Run: `mvn spotbugs:check`
3. View detailed results: `mvn spotbugs:gui`

### Current Status

- ✅ **All SpotBugs issues resolved** (0 bugs found)
- ✅ **18 unit tests passing**
- ✅ **Code quality analysis active**
- ✅ **Exclusion file configured** for false positives

### Java Version Compatibility

- ✅ **Java 11-21**: Fully supported
- ❌ **Java 23**: Not supported (use Java 11-21 for SpotBugs analysis)

## Limitations

- SQS message size limit of 256KB (connector uses 200KB for safety)
- No built-in message ordering (use FIFO queues if ordering is required)
- Limited to single queue per connector instance
- Requires CDC to be enabled on source table

## Support

For issues and questions:
- Check the logs for detailed error information
- Verify SQS queue configuration and permissions
- Ensure AWS credentials and permissions are correct
- Review Amazon SQS documentation for service limits and best practices
