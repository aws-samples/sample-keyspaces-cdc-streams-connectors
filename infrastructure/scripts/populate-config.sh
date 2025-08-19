#!/bin/bash
set -e

# Script to populate terraform.tfvars with actual AWS values
echo "🔧 Populating configuration with AWS values..."

# Get AWS account and region
AWS_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
DEFAULT_REGION=$(aws configure get region)

if [ -n "$DEFAULT_REGION" ]; then
    read -r -p "Enter AWS region (default: $DEFAULT_REGION, press Enter to proceed): " AWS_REGION
    AWS_REGION=${AWS_REGION:-$DEFAULT_REGION}
else
    read -r -p "Enter AWS region (e.g., us-east-1): " AWS_REGION
    if [ -z "$AWS_REGION" ]; then
        echo "❌ AWS region is required"
        exit 1
    fi
fi

# Copy example and populate with actual values
cd "$(dirname "$0")/../terraform"
cp terraform.tfvars.example terraform.tfvars

# Replace placeholders with actual values
sed -i.bak "s/<your-region>/$AWS_REGION/g" terraform.tfvars
sed -i.bak "s/<account-id>/$AWS_ACCOUNT/g" terraform.tfvars
sed -i.bak "s/<region>/$AWS_REGION/g" terraform.tfvars
rm terraform.tfvars.bak

echo "✅ terraform.tfvars populated with:"
echo "   AWS Account: $AWS_ACCOUNT"
echo "   AWS Region: $AWS_REGION"
echo ""
echo "⚠️  Please edit terraform.tfvars to add your actual:"
echo "   - Keyspace names (replace <keyspace>)"
echo "   - Table names (replace <table>, <table2>)"
echo "   - Stream timestamps (replace <timestamp>)"