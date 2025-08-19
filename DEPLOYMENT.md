# Keyspaces CDC to S3 Deployment Guide

## Prerequisites

- AWS CLI configured with appropriate permissions
- Docker installed
- Terraform installed
- Keyspaces tables with CDC enabled
- Java 8+ and Maven 3.6+ (for building dependencies)

## Quick Deployment (Automated)

```bash
# Run the automated deployment script (includes dependency setup)
./infrastructure/scripts/deploy.sh
```

Optional: Override AWS region if needed:

```bash
export AWS_REGION=ap-south-1
./infrastructure/scripts/deploy.sh
```

The script will:
- **Setup dependencies**: Clone and build AWS Keyspaces Streams Kinesis Adapter
- Auto-detect AWS account and region
- Prompt for stream ARNs if not configured
- Build JAR and Docker image
- Create ECR repository if needed
- Generate terraform.tfvars automatically
- Deploy infrastructure with confirmation

## Manual Deployment

### 1. Setup Dependencies

First, set up the AWS Keyspaces Streams Kinesis Adapter dependency:

```bash
# Clone and build the AWS library locally
./infrastructure/scripts/setup-dependencies.sh
```

### 2. Configure Stream ARNs

Use the configuration script to populate AWS values:

```bash
# Run the configuration script
./infrastructure/scripts/populate-config.sh

# Then edit terraform.tfvars to add your stream ARNs
vim infrastructure/terraform/terraform.tfvars
```

The script will automatically populate your AWS account ID and region, then you need to manually add your Keyspaces stream ARNs.

### 3. Deploy Infrastructure

```bash
cd infrastructure/terraform
terraform init
terraform plan
terraform apply
```

## 4. Verify Deployment

```bash
# Get outputs from Terraform
cd infrastructure/terraform
terraform output

# Check ECS services
aws ecs list-services --cluster keyspaces-streamer-cluster

# Check DynamoDB tables (created automatically)
aws dynamodb list-tables

# Check logs
aws logs describe-log-streams --log-group-name "/ecs/keyspaces-streamer"

# Check S3 data (use bucket name from terraform output)
aws s3 ls s3://$(terraform output -raw data_bucket_name)/ --recursive
```

## Key Features

- **Persistent Checkpoints**: DynamoDB tables maintain stream position across deployments
- **Auto-scaling**: ECS Fargate handles scaling automatically  
- **Monitoring**: CloudWatch logs and metrics included
- **Multi-stream**: Supports multiple Keyspaces streams simultaneously

## Troubleshooting

- **Architecture errors**: Ensure Docker images built with `--platform linux/amd64`
- **Permission errors**: Verify IAM role has Keyspaces, S3, and DynamoDB permissions
- **Stream errors**: Check stream ARNs are valid and CDC is enabled
- **Checkpoint issues**: DynamoDB table names must be consistent across deployments