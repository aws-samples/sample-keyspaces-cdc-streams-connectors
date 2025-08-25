# Infrastructure Deployment

This folder contains all the infrastructure components needed to deploy the Keyspaces CDC Streams connectors.

## Files Overview

- **Dockerfile**: Container definition for the KCL application
- **cloudformation-template.yaml**: AWS CloudFormation template for deploying the infrastructure
- **parameters.json**: Configuration parameters for the CloudFormation deployment
- **create-stack-with-json.sh**: Script to deploy the CloudFormation stack
- **move-docker-to-ecr.sh**: Script to build and push the Docker image to ECR

## Deployment Steps

### 1. Build and Push Docker Image to ECR

The following script will build this project and move the container image to ECR:

```bash
./move-docker-to-ecr.sh
```

### 2. Configure Parameters

The parameters file will allow you to set paramters for cloudformation template. Not all of the options for the Keyspaces connector are exposed in the cloudformation template. To add more paramters you will need to add environment varriable to the ECS task definition. Any option in the Keyspaces connector project application.conf can be overridden with an envrionment varriable. 

Edit the `parameters.json` file to configure your deployment:

```bash
vi parameters.json
```

#### Available Parameters 

The following parameters can be configured in the `parameters.json` file:

##### Network Configuration
- **VPCId**: The ID of the VPC where the ECS tasks will be deployed
- **PrivateSubnetIds**: Comma-separated list of private subnet IDs for the ECS tasks
- **SecurityGroupIds**: Comma-separated list of security group IDs for the ECS tasks
- **RouteTableId**: The route table ID for network routing configuration

##### Keyspaces Configuration
- **KeyspaceName**: The name of the Amazon Keyspaces keyspace containing the table with CDC enabled
- **TableName**: The name of the table in the keyspace that has CDC streams enabled
- **KeyspacesRegion**: The AWS region where the Keyspaces keyspace is located

##### Application Configuration
- **ApplicationName**: The name of the KCL application that will process the CDC streams
- **ServiceReplicaCount**: Number of ECS task replicas to run (default: 2)
- **Cpu**: CPU units allocated to each ECS task (1024 = 1 vCPU)
- **Memory**: Memory allocated to each ECS task in MB

##### S3 Configuration
- **S3BucketId**: The name of the S3 bucket where CDC data will be stored
- **S3Region**: The AWS region where the S3 bucket is located
- **S3Format**: The format for storing data in S3 (supported: "json", "avro")

##### Container Configuration
- **ECRImage**: The ECR image URI for the container that will process the streams

##### VPC Endpoint Configuration
####### You can disable creation if you already have an endpoint configured. 
- **CreateKeyspacesEndpoint**: Whether to create a VPC endpoint for Amazon Keyspaces (true/false)
- **CreateKeyspacesStreamsEndpoint**: Whether to create a VPC endpoint for Keyspaces CDC Streams (true/false)
- **CreateECREndpoints**: Whether to create VPC endpoints for ECR (true/false)
- **CreateSTSEndpoint**: Whether to create a VPC endpoint for AWS STS (true/false)
- **CreateCloudWatchEndpoints**: Whether to create VPC endpoints for CloudWatch (true/false)
- **CreateKinesisEndpoint**: Whether to create a VPC endpoint for Kinesis (true/false)
- **CreateDynamoDBEndpoint**: Whether to create a VPC endpoint for DynamoDB (true/false)
- **CreateS3Endpoint**: Whether to create a VPC endpoint for S3 (true/false)

### 3. Deploy Infrastructure

Execute the following script to deploy the CloudFormation template:

```bash
./create-stack-with-json.sh
```

## Prerequisites

Before deploying, ensure you have:

1. **AWS CLI configured** with appropriate permissions
2. **Docker installed** and running
3. **Amazon Keyspaces keyspace** with CDC-enabled table
4. **S3 bucket** for storing CDC data
5. **VPC with private subnets** for ECS tasks
6. **Security groups** configured for the ECS tasks

## Architecture

The CloudFormation template deploys:

- **ECS Cluster**: To run the KCL application
- **ECS Service**: With the specified number of task replicas
- **Task Definition**: With the container image and resource allocation
- **IAM Roles**: For ECS tasks to access AWS services
- **VPC Endpoints**: For secure communication with AWS services (optional)
- **CloudWatch Logs**: For application logging

## Cleanup

To remove the deployed infrastructure:

```bash
aws cloudformation delete-stack --stack-name <your-stack-name>
```

Replace `<your-stack-name>` with the actual stack name used during deployment.
