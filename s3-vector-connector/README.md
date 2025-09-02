# S3 Vector Connector for Amazon Keyspaces CDC Streams

This module provides a connector that writes Amazon Keyspaces CDC (Change Data Capture) records to Amazon S3 Vector Index with vector embeddings. It uses Amazon Bedrock to generate embeddings from text fields and stores them in S3 Vector Store for similarity search and vector operations.

## Features

- **Vector Embeddings**:  Generation of vector embeddings using Amazon Bedrock
- **S3 Vector Index Integration**: Direct integration with Amazon S3 Vector Index
- **Flexible Field Mapping**: Configurable embedding field, key field, and metadata fields
- **Multiple Data Types**: Support for various CQL data types in metadata
- **Retry Logic**: Configurable retry attempts with exponential backoff
- **Configurable Dimensions**: Support for different vector dimensions

## Prerequisites

- Java 11 or higher
- Maven 3.6 or higher
- AWS credentials configured (IAM role, credentials file, or environment variables)
- Amazon S3 Vector Store bucket with appropriate permissions
- Amazon Bedrock access for embedding generation
- Amazon Keyspaces table with CDC enabled

## Installation

You can use this library in the Keyspaces Connector project by including the dependency jar in the class path and setting the target mapper to the S3VectorTargetMapper class. 

```xml
<dependency>
    <groupId>software.amazon.ssa</groupId>
    <artifactId>s3-vector-connector</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Configuration

Configure the connector in your `application.conf` file:

```hocon
keyspaces-cdc-streams {
    connector {
        target-mapper = "software.amazon.ssa.streams.connector.s3.S3VectorTargetMapper"
        
        # Required: S3 Vector bucket name
        bucket-id = "my-vector-bucket"
        
        # Required: S3 Vector index name
        index-name = "my-vector-index"
       
        # Required: AWS region (default: us-east-1)
        region = "us-east-1"
        
         # Required: Field name to generate embeddings from
        embedding-field = "description"
        
        # Required: Field name to use as vector key
        key-field = "id"

         # Optional: List of fields to include as metadata
        metadata-fields = ["title", "category", "timestamp"]
       
        # Optional: Maximum retry attempts for S3 Vector operations (default: 3)
        max-retries = 3
        
        # Optional: Bedrock model for generating embeddings (default: amazon.titan-embed-text-v2:0)
        embedding-model = "amazon.titan-embed-text-v2:0"
        
        # Optional: Vector dimensions (default: 256)
        dimensions = 256
    }
}
```

### Configuration Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `bucket-id` | Yes | - | S3 Vector bucket name where vectors will be stored |
| `region` | Yes  | "us-east-1" | AWS region for S3 Vector and Bedrock operations |
| `max-retries` | No | 3 | Maximum retry attempts for S3 Vector operations |
| `embedding-model` | No | "amazon.titan-embed-text-v2:0" | Bedrock model for generating embeddings |
| `index-name` | Yes | - | S3 Vector index name |
| `embedding-field` | Yes | - | Field name to generate embeddings from |
| `key-field` | Yes | - | Field name to use as vector key |
| `metadata-fields` | No | [] | List of fields to include as metadata |
| `dimensions` | No | 256 | Vector dimensions |

## Supported Bedrock Models

The connector supports various Bedrock embedding models:

- `amazon.titan-embed-text-v2:0` (default)
- `amazon.titan-embed-text-v1`
- `cohere.embed-english-v3`
- `cohere.embed-multilingual-v3`

## Data Type Support

### Embedding Field
- **Supported**: `text`, `varchar`, `ascii`
- **Unsupported**: All other CQL types

### Key Field
- **Supported**: `text`, `varchar`, `ascii`
- **Unsupported**: All other CQL types

### Metadata Fields
- **Supported**: `text`, `varchar`, `ascii`, `int`, `bigint`, `float`, `double`, `boolean`
- **Unsupported**: `blob`, `timestamp`, `uuid`, and other complex types

