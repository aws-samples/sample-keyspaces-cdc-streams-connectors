terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}



# KMS Key for S3 Encryption
resource "aws_kms_key" "s3_key" {
  description             = "KMS key for S3 bucket encryption"
  deletion_window_in_days = 7
  enable_key_rotation     = true

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "Enable IAM User Permissions"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
        }
        Action   = "kms:*"
        Resource = "*"
      },
      {
        Sid    = "Allow S3 Service"
        Effect = "Allow"
        Principal = {
          Service = "s3.amazonaws.com"
        }
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*",
          "kms:DescribeKey"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_kms_alias" "s3_key" {
  name          = "alias/keyspaces-streamer-s3"
  target_key_id = aws_kms_key.s3_key.key_id
}


# S3 Bucket
resource "aws_s3_bucket" "keyspaces_data" {
  bucket = "keyspaces-data-${random_id.suffix.hex}"
}

# S3 Public Access Block
resource "aws_s3_bucket_public_access_block" "keyspaces_data" {
  bucket = aws_s3_bucket.keyspaces_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# S3 Versioning
resource "aws_s3_bucket_versioning" "keyspaces_data" {
  bucket = aws_s3_bucket.keyspaces_data.id
  versioning_configuration {
    status = "Disabled"
  }
}

# S3 Server Side Encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "keyspaces_data" {
  bucket = aws_s3_bucket.keyspaces_data.id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.s3_key.arn
      sse_algorithm     = "aws:kms"
    }
  }
}

# S3 Lifecycle Configuration
resource "aws_s3_bucket_lifecycle_configuration" "keyspaces_data" {
  bucket = aws_s3_bucket.keyspaces_data.id

  rule {
    id     = "transition_to_ia"
    status = "Enabled"

    filter {}

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }

    noncurrent_version_transition {
      noncurrent_days = 30
      storage_class   = "STANDARD_IA"
    }

    noncurrent_version_expiration {
      noncurrent_days = 365
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

# SNS Topic for S3 Event Notifications
resource "aws_sns_topic" "s3_events" {
  name              = "keyspaces-s3-events-${random_id.suffix.hex}"
  kms_master_key_id = aws_kms_key.s3_key.arn
}

resource "aws_sns_topic_policy" "s3_events" {
  arn = aws_sns_topic.s3_events.arn

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "s3.amazonaws.com"
        }
        Action   = "SNS:Publish"
        Resource = aws_sns_topic.s3_events.arn
        Condition = {
          StringEquals = {
            "aws:SourceAccount" = data.aws_caller_identity.current.account_id
          }
          ArnEquals = {
            "aws:SourceArn" = aws_s3_bucket.keyspaces_data.arn
          }
        }
      }
    ]
  })
}

# S3 Event Notification
resource "aws_s3_bucket_notification" "keyspaces_data" {
  bucket = aws_s3_bucket.keyspaces_data.id

  topic {
    topic_arn = aws_sns_topic.s3_events.arn
    events    = ["s3:ObjectCreated:*"]
  }

  depends_on = [aws_sns_topic_policy.s3_events]
}











resource "random_id" "suffix" {
  byte_length = 4
}

# ECS Cluster
resource "aws_ecs_cluster" "keyspaces_cluster" {
  name = "keyspaces-streamer-cluster"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

# IAM Role
resource "aws_iam_role" "ecs_task_role" {
  name = "keyspaces-streamer-task-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "ecs_task_policy" {
  name = "keyspaces-streamer-policy"
  role = aws_iam_role.ecs_task_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "cassandra:Select",
          "cassandra:DescribeStream",
          "cassandra:GetRecords",
          "cassandra:GetShardIterator",
          "cassandra:GetStream",
          "cassandra:ListStreams"
        ]
        Resource = "arn:aws:cassandra:${var.aws_region}:${data.aws_caller_identity.current.account_id}:*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject"
        ]
        Resource = [
          "${aws_s3_bucket.keyspaces_data.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.keyspaces_data.arn
        ]
      },

      {
        Effect = "Allow"
        Action = [
          "dynamodb:CreateTable",
          "dynamodb:DescribeTable",
          "dynamodb:UpdateTable",
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem",
          "dynamodb:Query",
          "dynamodb:Scan"
        ]
        Resource = "arn:aws:dynamodb:${var.aws_region}:*:table/keyspaces-streamer-*"
      },
      {
        Effect = "Allow"
        Action = [
          "cloudwatch:PutMetricData"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = [
          "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/ecs/keyspaces-streamer*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*",
          "kms:DescribeKey"
        ]
        Resource = [
          aws_kms_key.s3_key.arn
        ]
      }
    ]
  })
}

# ECS Task Definition for each stream ARN
resource "aws_ecs_task_definition" "keyspaces_streamer" {
  for_each = toset(var.stream_arns)

  family                   = "keyspaces-streamer-${substr(sha256(each.value), 0, 8)}"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "1024"
  memory                   = "2048"
  execution_role_arn       = aws_iam_role.ecs_execution_role.arn
  task_role_arn            = aws_iam_role.ecs_task_role.arn

  container_definitions = jsonencode([
    {
      name      = "keyspaces-streamer"
      image     = var.container_image
      essential = true
      command = [
        "java", "-jar", "/app/app.jar",
        each.value,
        aws_s3_bucket.keyspaces_data.bucket,
        var.aws_region
      ]
      environment = [
        {
          name  = "AWS_REGION"
          value = var.aws_region
        }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.keyspaces_streamer.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "ecs-${substr(sha256(each.value), 0, 8)}"
        }
      }
    }
  ])
}

resource "aws_iam_role" "ecs_execution_role" {
  name = "keyspaces-streamer-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "ecs_execution_role_policy" {
  role       = aws_iam_role.ecs_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# Variable for stream ARNs
variable "stream_arns" {
  description = "List of Keyspaces stream ARNs to process"
  type        = list(string)
  default     = []
}

# ECS Service for each stream ARN
resource "aws_ecs_service" "keyspaces_streamer" {
  for_each = toset(var.stream_arns)

  name            = "keyspaces-streamer-${substr(sha256(each.value), 0, 8)}"
  cluster         = aws_ecs_cluster.keyspaces_cluster.id
  task_definition = aws_ecs_task_definition.keyspaces_streamer[each.key].arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = [aws_subnet.private.id]
    assign_public_ip = false
    security_groups  = [aws_security_group.ecs_tasks.id]
  }
}

# VPC Configuration
resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "keyspaces-streamer-vpc"
  }
}

# VPC Flow Logs
resource "aws_flow_log" "vpc_flow_log" {
  iam_role_arn    = aws_iam_role.flow_log.arn
  log_destination = aws_cloudwatch_log_group.vpc_flow_log.arn
  traffic_type    = "ALL"
  vpc_id          = aws_vpc.main.id
}

resource "aws_cloudwatch_log_group" "vpc_flow_log" {
  name              = "/aws/vpc/flowlogs"
  retention_in_days = 365
  kms_key_id        = aws_kms_key.cloudwatch_logs.arn
}

resource "aws_iam_role" "flow_log" {
  name = "keyspaces-streamer-flow-log-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "vpc-flow-logs.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "flow_log" {
  name = "keyspaces-streamer-flow-log-policy"
  role = aws_iam_role.flow_log.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogGroups",
          "logs:DescribeLogStreams"
        ]
        Effect = "Allow"
        Resource = [
          "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/vpc/flowlogs*"
        ]
      }
    ]
  })
}

# Restrict Default Security Group
resource "aws_default_security_group" "default" {
  vpc_id = aws_vpc.main.id

  # No ingress or egress rules - completely restrictive
  tags = {
    Name = "keyspaces-streamer-default-sg-restricted"
  }
}



# Private Subnet for ECS Tasks
resource "aws_subnet" "private" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.1.0/24"
  availability_zone = data.aws_availability_zones.available.names[0]

  tags = {
    Name = "keyspaces-streamer-private"
  }
}

# Route Table for Private Subnet
resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "keyspaces-streamer-private-rt"
  }
}

# Route Table Association
resource "aws_route_table_association" "private" {
  subnet_id      = aws_subnet.private.id
  route_table_id = aws_route_table.private.id
}

# VPC Endpoints for AWS Services
resource "aws_vpc_endpoint" "s3" {
  vpc_id          = aws_vpc.main.id
  service_name    = "com.amazonaws.${var.aws_region}.s3"
  route_table_ids = [aws_route_table.private.id]

  tags = {
    Name = "keyspaces-streamer-s3-endpoint"
  }
}

resource "aws_vpc_endpoint" "dynamodb" {
  vpc_id          = aws_vpc.main.id
  service_name    = "com.amazonaws.${var.aws_region}.dynamodb"
  route_table_ids = [aws_route_table.private.id]

  tags = {
    Name = "keyspaces-streamer-dynamodb-endpoint"
  }
}

resource "aws_vpc_endpoint" "ecr_api" {
  vpc_id              = aws_vpc.main.id
  service_name        = "com.amazonaws.${var.aws_region}.ecr.api"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [aws_subnet.private.id]
  security_group_ids  = [aws_security_group.vpc_endpoints.id]
  private_dns_enabled = true

  tags = {
    Name = "keyspaces-streamer-ecr-api-endpoint"
  }
}

resource "aws_vpc_endpoint" "ecr_dkr" {
  vpc_id              = aws_vpc.main.id
  service_name        = "com.amazonaws.${var.aws_region}.ecr.dkr"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [aws_subnet.private.id]
  security_group_ids  = [aws_security_group.vpc_endpoints.id]
  private_dns_enabled = true

  tags = {
    Name = "keyspaces-streamer-ecr-dkr-endpoint"
  }
}

resource "aws_vpc_endpoint" "logs" {
  vpc_id              = aws_vpc.main.id
  service_name        = "com.amazonaws.${var.aws_region}.logs"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [aws_subnet.private.id]
  security_group_ids  = [aws_security_group.vpc_endpoints.id]
  private_dns_enabled = true

  tags = {
    Name = "keyspaces-streamer-logs-endpoint"
  }
}

resource "aws_vpc_endpoint" "monitoring" {
  vpc_id              = aws_vpc.main.id
  service_name        = "com.amazonaws.${var.aws_region}.monitoring"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [aws_subnet.private.id]
  security_group_ids  = [aws_security_group.vpc_endpoints.id]
  private_dns_enabled = true

  tags = {
    Name = "keyspaces-streamer-monitoring-endpoint"
  }
}

resource "aws_vpc_endpoint" "cassandra" {
  vpc_id              = aws_vpc.main.id
  service_name        = "com.amazonaws.${var.aws_region}.cassandra"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [aws_subnet.private.id]
  security_group_ids  = [aws_security_group.vpc_endpoints.id]
  private_dns_enabled = true

  tags = {
    Name = "keyspaces-streamer-cassandra-endpoint"
  }
}

resource "aws_vpc_endpoint" "cassandra_streams" {
  vpc_id              = aws_vpc.main.id
  service_name        = "api.aws.${var.aws_region}.cassandra-streams"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [aws_subnet.private.id]
  security_group_ids  = [aws_security_group.vpc_endpoints.id]
  private_dns_enabled = true

  tags = {
    Name = "keyspaces-streamer-cassandra-streams-endpoint"
  }
}

# Security Group for VPC Endpoints
resource "aws_security_group" "vpc_endpoints" {
  name_prefix = "keyspaces-streamer-endpoints-"
  description = "Security group for VPC endpoints"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.main.cidr_block]
    description = "HTTPS from VPC"
  }

  tags = {
    Name = "keyspaces-streamer-vpc-endpoints"
  }
}

data "aws_availability_zones" "available" {
  state = "available"
}

# Security Group for ECS Tasks
resource "aws_security_group" "ecs_tasks" {
  name_prefix = "keyspaces-streamer-"
  description = "Security group for Keyspaces streamer ECS tasks"
  vpc_id      = aws_vpc.main.id

  egress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "HTTPS outbound for AWS services"
  }

  egress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "HTTP outbound for package downloads"
  }

  tags = {
    Name = "keyspaces-streamer-ecs-tasks"
  }
}

# KMS Key for CloudWatch Logs
resource "aws_kms_key" "cloudwatch_logs" {
  description             = "KMS key for CloudWatch logs encryption"
  deletion_window_in_days = 7
  enable_key_rotation     = true

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "Enable IAM User Permissions"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
        }
        Action   = "kms:*"
        Resource = "*"
      },
      {
        Sid    = "Allow CloudWatch Logs"
        Effect = "Allow"
        Principal = {
          Service = "logs.${var.aws_region}.amazonaws.com"
        }
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*",
          "kms:DescribeKey"
        ]
        Resource = "*"
        Condition = {
          ArnLike = {
            "kms:EncryptionContext:aws:logs:arn" = [
              "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/ecs/keyspaces-streamer*",
              "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/vpc/flowlogs*"
            ]
          }
        }
      }
    ]
  })
}

resource "aws_kms_alias" "cloudwatch_logs" {
  name          = "alias/keyspaces-streamer-logs"
  target_key_id = aws_kms_key.cloudwatch_logs.key_id
}

# CloudWatch Log Group
resource "aws_cloudwatch_log_group" "keyspaces_streamer" {
  name              = "/ecs/keyspaces-streamer"
  retention_in_days = 365
  kms_key_id        = aws_kms_key.cloudwatch_logs.arn
}

# Data sources
data "aws_caller_identity" "current" {}

# Variables
variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "ap-south-1"
}



variable "container_image" {
  description = "Container image for the streamer"
  type        = string
}

# Outputs
output "data_bucket_name" {
  value = aws_s3_bucket.keyspaces_data.bucket
}





output "s3_events_topic_arn" {
  value = aws_sns_topic.s3_events.arn
}

output "ecs_cluster_name" {
  value = aws_ecs_cluster.keyspaces_cluster.name
}

output "ecs_service_names" {
  value = [for service in aws_ecs_service.keyspaces_streamer : service.name]
}

output "task_role_arn" {
  value = aws_iam_role.ecs_task_role.arn
}

output "log_group_name" {
  value = aws_cloudwatch_log_group.keyspaces_streamer.name
}

output "dynamodb_table_names" {
  value = [for arn in var.stream_arns :
    "keyspaces-streamer-${split("/", arn)[2]}-${split("/", arn)[4]}"
  ]
  description = "Expected DynamoDB table names created by the application"
}