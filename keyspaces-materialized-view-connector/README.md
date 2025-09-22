# Amazon Keyspaces Materialized View Connector

The Amazon Keyspaces Materialized View Connector is a specialized component for the KCL Keyspaces CDC Streams framework that enables real-time materialized view updates in Amazon Keyspaces. This connector processes Change Data Capture (CDC) events from source tables and automatically updates materialized views that can have different partition keys and clustering columns.

## Overview

Materialized views in Apache Cassandra provide an efficient way to query data using different partition keys than the base table. This connector will models materialized view behavior by automatically keeps materialized views in sync with their source tables by processing CDC events and applying the appropriate INSERT, UPDATE, and DELETE operations.

## Features

- **Real-time Synchronization**: Automatically maintains materialized views as source tables change
- **CDC Event Processing**: Processes INSERT, UPDATE, and DELETE operations from CDC streams
- **Configurable Field Selection**: Choose which fields to include in the materialized view
- **Retry Logic**: Built-in exponential backoff retry mechanism for failed operations
- **AWS Integration**: Native integration with Amazon Keyspaces using SigV4 authentication
- **Connection Pooling**: Optimized connection management for high-throughput scenarios

## Architecture

```
Source Table (CDC Enabled)
         ↓
   CDC Stream Events
         ↓
KeyspacesViewTargetMapper
         ↓
Materialized View Table
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
        application-name = "materialized-view-sync"
    }
    
    connector {
        target-mapper = "software.amazon.ssa.streams.connector.KeyspacesViewTargetMapper"
        keyspace-name = "your_keyspace"
        table-name = "materialized_view_table"
        fields-to-include = ["field1", "field2", "field3"]
        driver-config = "keyspaces-application.conf"
        max-retries = 3
    }
    
    coordinator {
        skip-shard-sync-at-worker-initialization-if-leases-exist = true
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

### Driver Configuration

The connector uses the `keyspaces-application.conf` file for Datastax Java Driver configuration:

```hocon
datastax-java-driver {
    basic {
        contact-points = ["cassandra.us-east-1.amazonaws.com:9142"]
        load-balancing-policy {
            local-datacenter = us-east-1
        }
        request.consistency = LOCAL_QUORUM
    }
    
    advanced.ssl-engine-factory {
        class = DefaultSslEngineFactory
        hostname-validation = false
    }
    
    advanced.auth-provider {
        class = software.aws.mcs.auth.SigV4AuthProvider
        region = us-east-1
    }
    
    advanced.connection.pool {
        local.size = 3
        remote.size = 1
    }
    
    advanced.request.timeout = 5 seconds
    advanced.connection.connect-timeout = 5 seconds
    advanced.connection.init-query-timeout = 5 seconds
    
    advanced.reconnection-policy {
        class = ExponentialReconnectionPolicy
        base-delay = 1 second
        max-delay = 60 seconds
    }
}
```

## Configuration Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `keyspace-name` | String | Yes | - | Target keyspace for the materialized view |
| `table-name` | String | Yes | - | Target materialized view table name |
| `fields-to-include` | List[String] | Yes | - | Fields to include in the materialized view |
| `partition-keys` | List[String] | Yes | - | Partition key fields for DELETE operations |
| `clustering-keys` | List[String] | Yes | - | Clustering key fields for DELETE operations |
| `driver-config` | String | No | `keyspaces-application.conf` | Datastax driver configuration file |
| `max-retries` | Integer | No | `3` | Maximum retry attempts for failed operations |

## Usage Example

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

### 2. Create a new table representing the View

```cql
CREATE TABLE IF NOT EXISTS media.movies_by_date (
    title text,
    overview text,
    original_lang text,
    rel_date date,
    popularity decimal,
    vote_count int,
    vote_average decimal,
    PRIMARY KEY (rel_date, title)
) WITH cdc = true
AND CUSTOM_PROPERTIES = {
  'cdc_specification': {'view_type':'NEW_AND_OLD_IMAGES'}
};
```

### 3. Configure Connector

```hocon
connector {
    target-mapper = "software.amazon.ssa.streams.connector.KeyspacesViewTargetMapper"
    keyspace-name = "media"
    table-name = "movies_by_date"
    fields-to-include = ["title", "overview", "original_lang", "rel_date", "popularity", "vote_count", "vote_average"]
    partition-keys = ["rel_date"]
    clustering-keys = ["title"]
    max-retries = 3
}
```

## How It Works

1. **CDC Event Processing**: The connector receives CDC events from the source table
2. **Operation Detection**: Determines the operation type (INSERT, UPDATE, DELETE, TTL, etc.)
3. **Field Extraction**: Extracts specified fields from the CDC event data
4. **Statement Generation**: 
   - For INSERT/UPDATE operations: Generates INSERT statements with all specified fields
   - For DELETE/TTL operations: Generates DELETE statements using partition and clustering keys
5. **Materialized View Update**: Executes the appropriate statement to update the materialized view
6. **Retry Logic**: Implements exponential backoff for failed operations
7. **Error Handling**: Logs errors and continues processing other records

### Supported Operations

| Operation Type | Action | Description |
|----------------|--------|-------------|
| `INSERT` | INSERT | Inserts new record into materialized view |
| `UPDATE` | INSERT | Updates record (Cassandra uses INSERT for updates) |
| `DELETE` | DELETE | Removes record from materialized view |
| `TTL` | DELETE | Removes record when TTL expires |
| `REPLICATED_INSERT` | INSERT | Handles replicated inserts |
| `REPLICATED_UPDATE` | INSERT | Handles replicated updates |
| `REPLICATED_DELETE` | DELETE | Handles replicated deletes |

## Error Handling

The connector includes robust error handling:

- **Retry Logic**: Exponential backoff with configurable maximum retries
- **Error Logging**: Detailed logging of failed operations
- **Graceful Degradation**: Continues processing other records when individual operations fail
- **Connection Management**: Automatic reconnection on connection failures

## Performance Considerations

- **Connection Pooling**: Uses connection pooling for optimal performance
- **Batch Processing**: Processes multiple records efficiently
- **Consistency Level**: Uses LOCAL_QUORUM for optimal performance and consistency
- **Retry Strategy**: Exponential backoff prevents overwhelming the system

## Monitoring and Logging

The connector provides comprehensive logging:

- **INFO Level**: Successful operations and initialization
- **WARN Level**: Retry attempts and recoverable errors
- **ERROR Level**: Failed operations and configuration issues
- **DEBUG Level**: Detailed operation information

## Prerequisites

- Amazon Keyspaces table with CDC enabled
- AWS credentials configured (IAM role, credentials file, or environment variables)
- Java 11 or higher
- Maven 3.6 or higher

## Dependencies

- `kcl-keyspaces-core`: Core KCL Keyspaces functionality
- `java-driver-core`: Datastax Java Driver for Cassandra
- `aws-sigv4-auth-cassandra-java-driver-plugin`: AWS authentication plugin
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

1. **Connection Failures**: Verify AWS credentials and network connectivity
2. **Permission Errors**: Ensure IAM role has necessary Keyspaces permissions
3. **Configuration Errors**: Check field names match the materialized view schema
4. **Retry Exhaustion**: Increase `max-retries` for high-latency scenarios

### Debug Mode

Enable debug logging by setting the log level:

```hocon
logger.keyspaces-view-connector = DEBUG
```

## Best Practices

1. **Field Selection**: Only include necessary fields to minimize data transfer
2. **Key Configuration**: Ensure partition-keys and clustering-keys match your materialized view schema exactly
3. **Retry Configuration**: Adjust retry settings based on your latency requirements
4. **Connection Pooling**: Tune connection pool settings for your workload
5. **Monitoring**: Set up CloudWatch alarms for error rates and latency
6. **Testing**: Test with small datasets before production deployment
7. **Schema Validation**: Verify that all fields in fields-to-include exist in both source and target tables
8. **Operation Handling**: Understand that UPDATE operations are handled as INSERT in Cassandra (upsert behavior)

## Advanced Configuration

### Multiple Materialized Views

You can configure multiple materialized views by running multiple instances of the connector with different configurations:

```hocon
# Instance 1: Movies by date
connector {
    target-mapper = "software.amazon.ssa.streams.connector.KeyspacesViewTargetMapper"
    keyspace-name = "media"
    table-name = "movies_by_date"
    fields-to-include = ["title", "overview", "original_lang", "rel_date", "popularity", "vote_count", "vote_average"]
    partition-keys = ["rel_date"]
    clustering-keys = ["title"]
}

# Instance 2: Movies by language
connector {
    target-mapper = "software.amazon.ssa.streams.connector.KeyspacesViewTargetMapper"
    keyspace-name = "media"
    table-name = "movies_by_language"
    fields-to-include = ["title", "overview", "original_lang", "rel_date", "popularity", "vote_count", "vote_average"]
    partition-keys = ["original_lang"]
    clustering-keys = ["title", "rel_date"]
}
```

### Custom Driver Configuration

You can create custom driver configurations for specific use cases:

```hocon
# keyspaces-custom.conf
datastax-java-driver {
    basic {
        contact-points = ["cassandra.us-east-1.amazonaws.com:9142"]
        load-balancing-policy {
            local-datacenter = us-east-1
        }
        request.consistency = LOCAL_QUORUM
    }
    
    advanced.connection.pool {
        local.size = 10  # Increased for high throughput
        remote.size = 2
    }
    
    advanced.request.timeout = 10 seconds  # Longer timeout for large operations
}
```

Then reference it in your connector configuration:

```hocon
connector {
    target-mapper = "software.amazon.ssa.streams.connector.KeyspacesViewTargetMapper"
    driver-config = "keyspaces-custom.conf"
    # ... other configuration
}
```

## Rebuilding the view
- Enable Keyspaces streams of source table
- Create target table in Amazon Keyspaces
- Deploy Keyspaces Materialized View Connector
- Bulk export full view of the base table to s3 [Export with AWS Glue and Spark](https://github.com/aws-samples/amazon-keyspaces-examples/tree/main/scala/datastax-v4/aws-glue/export-to-s3)
- Bulk import historical changes using [Import AWS Glue and Spark](https://github.com/aws-samples/amazon-keyspaces-examples/blob/main/scala/datastax-v4/aws-glue/import-from-s3/import-sample.scala)
- For import set spark.cassandra.output.ifNotExists to ture, to only write records which do not exists. 

## Limitations

- All fields in fields-to-include must exist in both source and target tables
- Partition and clustering keys must be configured correctly for DELETE operations
- TTL operations are handled as DELETE operations (records are removed when TTL expires)


## Support

For issues and questions:
- Check the logs for detailed error information
- Verify configuration parameters
- Ensure AWS credentials and permissions are correct
- Review Amazon Keyspaces documentation for CDC requirements
