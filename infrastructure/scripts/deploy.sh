#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}🚀 Keyspaces CDC to S3 Deployment Script${NC}"

# Verify project can build
echo -e "\n${YELLOW}🔍 Verifying project build...${NC}"
if ! mvn clean package -DskipTests -q; then
    echo -e "${RED}❌ Project build failed. Please check your code and dependencies.${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Project builds successfully${NC}"

# Get AWS account and region dynamically
AWS_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
DEFAULT_REGION=${AWS_REGION:-$(aws configure get region)}

if [ -n "$DEFAULT_REGION" ]; then
    read -r -p "Enter AWS region (default: $DEFAULT_REGION, press Enter to proceed): " AWS_REGION
    AWS_REGION=${AWS_REGION:-$DEFAULT_REGION}
else
    read -r -p "Enter AWS region (e.g., us-east-1): " AWS_REGION
    if [ -z "$AWS_REGION" ]; then
        echo -e "${RED}❌ AWS region is required${NC}"
        exit 1
    fi
fi

ECR_REPO_NAME="keyspaces-streamer"
IMAGE_TAG=${IMAGE_TAG:-latest}

echo -e "${YELLOW}Using AWS Account: $AWS_ACCOUNT${NC}"
echo -e "${YELLOW}Using AWS Region: $AWS_REGION${NC}"

# Check if terraform.tfvars exists, if not prompt for stream ARNs
if [ ! -f "infrastructure/terraform/terraform.tfvars" ]; then
    echo -e "\n${YELLOW}terraform.tfvars not found. Enter Keyspaces stream ARNs:${NC}"
    STREAM_ARNS=()
    while true; do
        read -r -p "Stream ARN (press Enter to finish): " arn
        if [ -z "$arn" ]; then
            break
        fi
        STREAM_ARNS+=("$arn")
    done
    
    if [ ${#STREAM_ARNS[@]} -eq 0 ]; then
        echo -e "${RED}❌ At least one stream ARN is required${NC}"
        exit 1
    fi
    
    # Generate terraform.tfvars with actual values
    cat > infrastructure/terraform/terraform.tfvars << EOF
aws_region = "$AWS_REGION"
container_image = "$AWS_ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPO_NAME:$IMAGE_TAG"
stream_arns = [
$(printf '  "%s",\n' "${STREAM_ARNS[@]}" | sed '$ s/,$//')
]
EOF
    echo -e "${GREEN}✅ Generated terraform.tfvars${NC}"
fi

echo -e "\n${GREEN}📦 Building JAR...${NC}"
mvn clean package -DskipTests

echo -e "\n${GREEN}🐳 Building Docker image...${NC}"
cp target/keyspaces-s3-streamer-1.0.0.jar infrastructure/docker/
cd infrastructure/docker

ECR_URI="${AWS_ACCOUNT}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO_NAME}"

# Create ECR repository if it doesn't exist
aws ecr describe-repositories --repository-names "${ECR_REPO_NAME}" --region "${AWS_REGION}" 2>/dev/null || \
aws ecr create-repository --repository-name "${ECR_REPO_NAME}" --region "${AWS_REGION}"

aws ecr get-login-password --region "${AWS_REGION}" | docker login --username AWS --password-stdin "${ECR_URI}"

docker build --platform linux/amd64 -t "${ECR_REPO_NAME}:${IMAGE_TAG}" . --no-cache
docker tag "${ECR_REPO_NAME}:${IMAGE_TAG}" "${ECR_URI}:${IMAGE_TAG}"
docker push "${ECR_URI}:${IMAGE_TAG}"

cd ../terraform

echo -e "\n${GREEN}🏗️  Deploying infrastructure...${NC}"
terraform init
terraform plan -out=tfplan
read -r -p "Apply changes? (y/N): " confirm
if [[ $confirm == [yY] ]]; then
    terraform apply tfplan
    rm -f tfplan
    echo -e "\n${GREEN}✅ Deployment completed!${NC}"
    echo -e "\n${YELLOW}Infrastructure outputs:${NC}"
    terraform output
else
    echo -e "${YELLOW}⚠️  Deployment cancelled${NC}"
    rm -f tfplan
fi