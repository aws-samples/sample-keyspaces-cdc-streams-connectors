#!/bin/bash

# Cleanup script for Keyspaces CDC Streams to S3 project
# This script removes all AWS resources created by the project

set -e

# Suppress AWS CLI pager and verbose outputs
export AWS_PAGER=""
export AWS_CLI_AUTO_PROMPT=off

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TERRAFORM_DIR="$SCRIPT_DIR/../terraform"

echo "🧹 Starting cleanup of Keyspaces CDC Streams to S3 resources..."

# Check if AWS CLI is configured
if ! aws sts get-caller-identity >/dev/null 2>&1; then
    echo "❌ AWS CLI not configured. Please run 'aws configure' first."
    exit 1
fi

# Get AWS region from terraform.tfvars or use default
REGION="ap-south-1"
if [ -f "$TERRAFORM_DIR/terraform.tfvars" ]; then
    REGION=$(grep "aws_region" "$TERRAFORM_DIR/terraform.tfvars" | cut -d'"' -f2 || echo "ap-south-1")
fi

echo "🌍 Using AWS region: $REGION"

# Function to get all Terraform-managed resources
get_terraform_resources() {
    LOG_GROUP=""
    KMS_KEY_IDS=""
    KMS_ALIAS_NAMES=""
    SNS_TOPIC_ARNS=""
    IAM_ROLE_NAMES=""
    
    if [ -f "$TERRAFORM_DIR/terraform.tfstate" ]; then
        cd "$TERRAFORM_DIR"
        
        # Check if terraform is initialized and state is valid
        if terraform state list >/dev/null 2>&1; then
            # Get outputs silently
            LOG_GROUP=$(terraform output -raw log_group_name 2>/dev/null || echo "")
            KMS_KEY_IDS=$(terraform output -json kms_key_ids 2>/dev/null | jq -r '.[]?' 2>/dev/null | tr '\n' ' ' || echo "")
            KMS_ALIAS_NAMES=$(terraform output -json kms_alias_names 2>/dev/null | jq -r '.[]?' 2>/dev/null | tr '\n' ' ' || echo "")
            SNS_TOPIC_ARNS=$(terraform output -json sns_topic_arns 2>/dev/null | jq -r '.[]?' 2>/dev/null | tr '\n' ' ' || echo "")
            IAM_ROLE_NAMES=$(terraform output -json iam_role_names 2>/dev/null | jq -r '.[]?' 2>/dev/null | tr '\n' ' ' || echo "")
        fi
        
        cd - >/dev/null
    fi
}

# Function to empty and delete S3 buckets
cleanup_s3_buckets() {
    echo "🗑️  Cleaning up S3 buckets..."
    
    # Get all buckets and filter for project-related ones
    ALL_BUCKETS=$(aws s3 ls 2>/dev/null | awk '{print $3}' | grep -E '(keyspaces-data-|keyspaces-cdc|keyspaces-streamer)' || echo "")
    
    if [ -n "$ALL_BUCKETS" ]; then
        for BUCKET in $ALL_BUCKETS; do
            echo "   Emptying and deleting bucket: $BUCKET"
            aws s3 rm "s3://$BUCKET" --recursive --region "$REGION" >/dev/null 2>&1 || true
            aws s3api delete-bucket --bucket "$BUCKET" --region "$REGION" >/dev/null 2>&1 || true
        done
    else
        echo "   No project buckets found"
    fi
}

# Function to delete DynamoDB tables
cleanup_dynamodb_tables() {
    echo "🗃️  Cleaning up DynamoDB tables..."
    
    # Get all tables and filter for project-related ones
    ALL_TABLES=$(aws dynamodb list-tables --region "$REGION" --output text --query 'TableNames[]' 2>/dev/null | tr '\t' '\n' | grep -E '(keyspaces-streamer-|keyspaces|media)' || echo "")
    
    if [ -n "$ALL_TABLES" ]; then
        for TABLE in $ALL_TABLES; do
            echo "   Deleting table: $TABLE"
            aws dynamodb delete-table --table-name "$TABLE" --region "$REGION" >/dev/null 2>&1 || true
        done
    else
        echo "   No project DynamoDB tables found"
    fi
}

# Function to delete ECR repositories
cleanup_ecr_repositories() {
    echo "🐳 Cleaning up ECR repositories..."
    
    # Get all repositories and filter for project-related ones
    ALL_REPOS=$(aws ecr describe-repositories --region "$REGION" --output text --query 'repositories[].repositoryName' 2>/dev/null | tr '\t' '\n' | grep -E '(keyspaces|streamer|cdc)' || echo "")
    
    if [ -n "$ALL_REPOS" ]; then
        for REPO in $ALL_REPOS; do
            echo "   Deleting repository: $REPO"
            aws ecr delete-repository --repository-name "$REPO" --force --region "$REGION" >/dev/null 2>&1 || true
        done
    else
        echo "   No project ECR repositories found"
    fi
}



# Function to run terraform destroy
cleanup_terraform() {
    echo "🏗️  Running Terraform destroy..."
    
    if [ -f "$TERRAFORM_DIR/terraform.tfstate" ]; then
        cd "$TERRAFORM_DIR"
        terraform destroy -auto-approve || true
        cd - >/dev/null
    else
        echo "   No terraform state found, skipping terraform destroy"
    fi
}

# Function to cleanup orphaned IAM roles (fallback only)
cleanup_orphaned_iam_roles() {
    echo "👤 Cleaning up orphaned IAM roles..."
    
    get_terraform_resources
    
    # Only clean roles that match our exact naming pattern (fallback)
    for ROLE in $IAM_ROLE_NAMES; do
        if [ -n "$ROLE" ]; then
            echo "   Cleaning up orphaned role: $ROLE"
            # Detach managed policies
            ATTACHED_POLICIES=$(aws iam list-attached-role-policies --role-name "$ROLE" --query 'AttachedPolicies[].PolicyArn' --output text 2>/dev/null || true)
            for POLICY in $ATTACHED_POLICIES; do
                aws iam detach-role-policy --role-name "$ROLE" --policy-arn "$POLICY" >/dev/null 2>&1 || true
            done
            
            # Delete inline policies
            INLINE_POLICIES=$(aws iam list-role-policies --role-name "$ROLE" --query 'PolicyNames' --output text 2>/dev/null || true)
            for POLICY in $INLINE_POLICIES; do
                aws iam delete-role-policy --role-name "$ROLE" --policy-name "$POLICY" >/dev/null 2>&1 || true
            done
            
            echo "   Deleting role: $ROLE"
            aws iam delete-role --role-name "$ROLE" >/dev/null 2>&1 || true
        fi
    done
}

# Function to cleanup SNS topics
cleanup_sns_topics() {
    echo "📢 Cleaning up SNS topics..."
    
    get_terraform_resources
    
    # Only delete topics managed by Terraform
    for TOPIC in $SNS_TOPIC_ARNS; do
        if [ -n "$TOPIC" ]; then
            echo "   Deleting SNS topic: $TOPIC"
            aws sns delete-topic --topic-arn "$TOPIC" --region "$REGION" >/dev/null 2>&1 || true
        fi
    done
}

# Function to cleanup KMS keys
cleanup_kms_keys() {
    echo "🔐 Cleaning up KMS keys..."
    
    get_terraform_resources
    
    # Delete KMS aliases managed by Terraform
    for ALIAS in $KMS_ALIAS_NAMES; do
        if [ -n "$ALIAS" ]; then
            echo "   Deleting KMS alias: $ALIAS"
            aws kms delete-alias --alias-name "$ALIAS" --region "$REGION" >/dev/null 2>&1 || true
        fi
    done
    
    # Disable and schedule deletion of KMS keys managed by Terraform
    for KEY in $KMS_KEY_IDS; do
        if [ -n "$KEY" ]; then
            echo "   Disabling and scheduling deletion of KMS key: $KEY"
            aws kms disable-key --key-id "$KEY" --region "$REGION" >/dev/null 2>&1 || true
            aws kms schedule-key-deletion --key-id "$KEY" --pending-window-in-days 7 --region "$REGION" >/dev/null 2>&1 || true
        fi
    done
}

# Function to cleanup CloudWatch log groups
cleanup_log_groups() {
    echo "📊 Cleaning up CloudWatch log groups..."
    
    get_terraform_resources
    
    # Delete log group managed by Terraform
    if [ -n "$LOG_GROUP" ]; then
        echo "   Deleting log group: $LOG_GROUP"
        aws logs delete-log-group --log-group-name "$LOG_GROUP" --region "$REGION" >/dev/null 2>&1 || true
    fi
    
    # Find and delete other project-related log groups
    PROJECT_LOG_GROUPS=$(aws logs describe-log-groups --region "$REGION" --query "logGroups[?contains(logGroupName, \`keyspaces\`) || contains(logGroupName, \`streamer\`)].logGroupName" --output text 2>/dev/null || true)
    
    for LOG_GROUP_NAME in $PROJECT_LOG_GROUPS; do
        if [ -n "$LOG_GROUP_NAME" ]; then
            echo "   Deleting log group: $LOG_GROUP_NAME"
            aws logs delete-log-group --log-group-name "$LOG_GROUP_NAME" --region "$REGION" >/dev/null 2>&1 || true
        fi
    done
}

# Main cleanup execution
echo "⚠️  This will delete ALL resources created by this project. Continue? (y/N)"
read -r CONFIRM
if [[ ! "$CONFIRM" =~ ^[Yy]$ ]]; then
    echo "❌ Cleanup cancelled"
    exit 0
fi

echo "🚀 Starting cleanup process..."

# Run cleanup functions in order
cleanup_s3_buckets
cleanup_dynamodb_tables
cleanup_ecr_repositories
cleanup_terraform
cleanup_orphaned_iam_roles
cleanup_sns_topics
cleanup_kms_keys
cleanup_log_groups

echo "✅ Cleanup completed successfully!"
echo "📝 Note: Some resources may take a few minutes to be fully deleted"
echo "🔑 KMS keys are scheduled for deletion in 7 days (minimum AWS requirement)"