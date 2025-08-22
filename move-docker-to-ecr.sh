#!/bin/bash

set -e  # Exit immediately if any command fails

REGION="us-east-1"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
AWS_REPO_NAME="amazon-keyspaces-streams"
IMAGE_TAG="latest"
IMAGE_NAME="aks-streams-connector"

# Authenticate with ECR
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com

# Create ECR repository (ignore error if it already exists)
aws ecr create-repository --repository-name ${AWS_REPO_NAME} --region ${REGION} || echo "ECR repo already exists"

mkdir -p certs

#curl -o certs/sf-class2-root.crt https://certs.secureserver.net/repository/sf-class2-root.crt

# Build locally
docker build -f Dockerfile --platform linux/amd64 -t ${IMAGE_NAME}:${IMAGE_TAG} .

docker tag ${IMAGE_NAME}:${IMAGE_TAG} ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${AWS_REPO_NAME}:${IMAGE_TAG}

# Push to ECR
docker push ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${AWS_REPO_NAME}:${IMAGE_TAG}
