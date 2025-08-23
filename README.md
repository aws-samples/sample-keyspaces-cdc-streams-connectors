# Keyspaces CDC Streams connectors

Move data from Amazon Keyspaces to different downstream targets using Keyspaces CDC Streams connectors. This library will allow you to deploy Kinesis client library (KCL)  based application to stream data from your Keyspaces. The goal is to simoplify connecting data source to different targets. 

## Current Available Connectors
- Connector to s3 with supported format json, avro. 


## Architecture

This project implements a streaming architecture that processes Amazon Keyspaces CDC events and stores them in S3. 

![Architecture Diagram](architecture/keyspaces-cdc-s3-architecture-numbered.png)

Detailed flow of architecture can be referred at [Architecture Description](architecture/ARCHITECTURE.md)

For step-by-step data flow visualization, see [Data Flow Diagram](architecture/DATA_FLOW.md)

## Quick Start

## Create a keyspace and a table with streams enabled

```cql
CREATE KEYSPACE media WITH replication = {'class': 'SingleRegionStrategy'};
```

```cql
CREATE TABLE media.media_content (
    content_id uuid,
    title text,
    creator_id uuid,
    media_type text,
    upload_timestamp timestamp,
    status text,
    PRIMARY KEY (content_id)
) WITH cdc = true
AND CUSTOM_PROPERTIES = {
  'cdc_specification': {
    'view_type': 'NEW_AND_OLD_IMAGES'
  }
};
```

### 2. Build project and move Container to Elastic container registry

The following script will build this project and move the container image to ECS. 
```bash
./move-docker-to-ecr.sh
```

### 3. Configure project 

Next you will need to configure the paramters file which will be passed into a cloudformation template to deploy the ECS task. 

```bash
vi parameters.json
  ```
## Available Parameters

The following parameters can be configured in the `parameters.json` file:

### Network Configuration
- **VPCId**: The ID of the VPC where the ECS tasks will be deployed
- **PrivateSubnetIds**: Comma-separated list of private subnet IDs for the ECS tasks
- **SecurityGroupIds**: Comma-separated list of security group IDs for the ECS tasks
- **RouteTableId**: The route table ID for network routing configuration

### Keyspaces Configuration
- **KeyspaceName**: The name of the Amazon Keyspaces keyspace containing the table with CDC enabled
- **TableName**: The name of the table in the keyspace that has CDC streams enabled
- **KeyspacesRegion**: The AWS region where the Keyspaces keyspace is located

### Application Configuration
- **ApplicationName**: The name of the KCL application that will process the CDC streams
- **ServiceReplicaCount**: Number of ECS task replicas to run (default: 2)
- **Cpu**: CPU units allocated to each ECS task (1024 = 1 vCPU)
- **Memory**: Memory allocated to each ECS task in MB

### S3 Configuration
- **S3BucketId**: The name of the S3 bucket where CDC data will be stored
- **S3Region**: The AWS region where the S3 bucket is located
- **S3Format**: The format for storing data in S3 (supported: "json", "avro")

### Container Configuration
- **ECRImage**: The ECR image URI for the container that will process the streams

### VPC Endpoint Configuration
- **CreateKeyspacesEndpoint**: Whether to create a VPC endpoint for Amazon Keyspaces (true/false)
- **CreateKeyspacesStreamsEndpoint**: Whether to create a VPC endpoint for Keyspaces CDC Streams (true/false)
- **CreateECREndpoints**: Whether to create VPC endpoints for ECR (true/false)
- **CreateSTSEndpoint**: Whether to create a VPC endpoint for AWS STS (true/false)
- **CreateCloudWatchEndpoints**: Whether to create VPC endpoints for CloudWatch (true/false)
- **CreateKinesisEndpoint**: Whether to create a VPC endpoint for Kinesis (true/false)
- **CreateDynamoDBEndpoint**: Whether to create a VPC endpoint for DynamoDB (true/false)
- **CreateS3Endpoint**: Whether to create a VPC endpoint for S3 (true/false)

### 4. Execute the CloudFormation template with the parameters file

execute the following script to deploy the cloudfromation template.
```
./ create-stack-with-json.sh
```


### 5. Test with Sample Data

Insert some test records:

```cql
INSERT INTO media.media_content (content_id, title, creator_id, media_type, upload_timestamp, status) VALUES (uuid(), 'Summer Vacation Video', uuid(), 'video', toTimestamp(now()), 'active');
INSERT INTO media.media_content (content_id, title, creator_id, media_type, upload_timestamp, status) VALUES (uuid(), 'Birthday Party Photos', uuid(), 'image', toTimestamp(now()), 'processing');
INSERT INTO media.media_content (content_id, title, creator_id, media_type, upload_timestamp, status) VALUES (uuid(), 'Podcast Episode 1', uuid(), 'audio', toTimestamp(now()), 'active');
```

### 6. Verify S3 Output

Records are stored in S3 in the bucket provided



## References

- [Amazon Keyspaces (for Apache Cassandra) now supports Change Data Capture (CDC) Streams](https://aws.amazon.com/about-aws/whats-new/2025/07/amazon-keyspaces-apache-cassandra-cdc-streams/)
- [Working with change data capture (CDC) streams in Amazon Keyspaces](https://docs.aws.amazon.com/keyspaces/latest/devguide/cdc.html)

### Service Best practices

- [Best practices for designing and architecting with Amazon Keyspaces (for Apache Cassandra)](https://docs.aws.amazon.com/keyspaces/latest/devguide/best-practices.html)
- [Amazon ECS best practices](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ecs-best-practices.html)
- [Best practices design patterns: optimizing Amazon S3 performance](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html)
- [Best practices for designing and architecting with DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/best-practices.html)

### Security best practices

- [Security best practices for Amazon Keyspaces](https://docs.aws.amazon.com/keyspaces/latest/devguide/best-practices-security.html)
- [Security best practices and use cases in AWS Identity and Access Management](https://docs.aws.amazon.com/IAM/latest/UserGuide/best-practices-use-cases.html)
- [Security best practices in IAM](https://docs.aws.amazon.com/IAM/latest/UserGuide/best-practices.html)
- [Security in Amazon Elastic Container Service](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/security.html)
- [Amazon S3 security best practices](https://docs.aws.amazon.com/AmazonS3/latest/userguide/security-best-practices.html)
- [Security and compliance in Amazon DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/security.html)


## Disclaimer

The sample code; software libraries; command line tools; proofs of concept; templates; or other related technology (including any of the foregoing that are provided by our personnel) is provided to you as AWS Content under the AWS Customer Agreement, or the relevant written agreement between you and AWS (whichever applies). You should not use this AWS Content in your production accounts, or on production or other critical data. You are responsible for testing, securing, and optimizing the AWS Content, such as sample code, as appropriate for production grade use based on your specific quality control practices and standards. Deploying AWS Content may incur AWS charges for creating or using AWS chargeable resources, such as running Amazon EC2 instances or using Amazon S3 storage.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidelines.

## License

This project is licensed under the MIT No Attribution - see the [LICENSE](LICENSE) file for details.
