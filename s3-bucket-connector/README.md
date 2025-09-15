# S3 Bucket Connector for Amazon Keyspaces CDC Streams

This module provides a connector that writes Amazon Keyspaces CDC (Change Data Capture) records to Amazon S3 in either Avro or JSON format. It supports time-based partitioning and configurable retry logic for reliable data delivery.

## Features

- **Multiple Output Formats**: Support for both Avro and JSON serialization
- **Time-based Partitioning**: Configurable partitioning by seconds, minutes, hours, days, months, or years
- **Retry Logic**: Configurable retry attempts with exponential backoff
- **Sequence Tracking**: Files are named with sequence number ranges for easy tracking
- **Flexible Configuration**: Configurable S3 bucket, prefix, region, and other parameters

## Prerequisites

- Java 11 or higher
- Maven 3.6 or higher
- AWS credentials configured (IAM role, credentials file, or environment variables)
- Amazon S3 bucket with appropriate permissions
- Amazon Keyspaces table with CDC enabled

## Installation

Add this module as a dependency to the kcl-keyspaces-app project and add the S3TargetMappper class to the configuration. 

```xml
<dependency>
    <groupId>software.amazon.ssa</groupId>
    <artifactId>s3-bucket-connector</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Configuration

Configure the connector in your `streams-application.conf` file:

```hocon
keyspaces-cdc-streams {
    connector {
        target-mapper = "software.amazon.ssa.streams.connector.s3.S3TargetMapper"
        
        # Required: S3 bucket name
        bucket-id = "my-keyspaces-cdc-bucket"
        
        # Optional: S3 key prefix for organizing files
        prefix = "export/keyspace_name/table_name/incremental"
        
        # Required: AWS region (default: us-east-1)
        region = "us-east-1"
        
        # Optional: Output format - "avro" or "json" (default: avro)
        format = "avro"
        
        # Optional: Time partitioning granularity (default: hours)
        # Options: "seconds", "minutes", "hours", "days", "months", "years"
        timestamp-partition = "hours"
        
        # Optional: Maximum retry attempts for S3 operations (default: 3)
        max-retries = 3
    }
}
```

### Configuration Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `bucket-id` | Yes | - | S3 bucket name where records will be stored |
| `prefix` | No | "" | S3 key prefix for organizing files |
| `region` | No | "us-east-1" | AWS region for S3 operations |
| `format` | No | "avro" | Output format: "avro" or "json" |
| `timestamp-partition` | No | "hours" | Time partitioning granularity |
| `max-retries` | No | 3 | Maximum retry attempts for S3 operations |

## File Structure

Files are stored in S3 with the following structure:

```
s3://bucket-name/prefix/partition/firstSequenceNumber-lastSequenceNumber-timestamp.extension
```

Example:
```
s3://my-bucket/export/keyspace/table/2024/01/15/14/1234567890-1234567899-1705324800000.avro
```

### Partitioning Options

- **seconds**: `YYYY/MM/DD/HH/MM/SS/`
- **minutes**: `YYYY/MM/DD/HH/MM/`
- **hours**: `YYYY/MM/DD/HH/`
- **days**: `YYYY/MM/DD/`
- **months**: `YYYY/MM/`
- **years**: `YYYY/`

## Output Formats

### Avro Format

The Avro format provides:
- Schema evolution capabilities
- Efficient binary serialization
- Type safety
- Compression benefits

### JSON Format

The JSON format provides:
- Human-readable output
- Easy parsing with standard JSON tools
- Simple structure for downstream processing

#
