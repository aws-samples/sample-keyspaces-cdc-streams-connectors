#!/bin/bash

# AWS CLI CloudFormation Stack Creation Script using JSON parameters file
# This approach is cleaner for complex parameter sets

# Configuration
STACK_NAME="kcl-keyspaces-runner3"
REGION="us-east-1"
TEMPLATE_FILE="cloudformation-template.yaml"
PARAMETERS_FILE="parameters.json"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    print_error "AWS CLI is not installed. Please install it first."
    exit 1
fi

# Check if template file exists
if [ ! -f "$TEMPLATE_FILE" ]; then
    print_error "CloudFormation template file '$TEMPLATE_FILE' not found."
    exit 1
fi

# Check if parameters file exists
if [ ! -f "$PARAMETERS_FILE" ]; then
    print_error "Parameters file '$PARAMETERS_FILE' not found."
    exit 1
fi

# Verify AWS credentials
print_status "Verifying AWS credentials..."
aws sts get-caller-identity > /dev/null
if [ $? -ne 0 ]; then
    print_error "AWS credentials are not properly configured. Please run 'aws configure' first."
    exit 1
fi

# Get AWS Account ID for validation
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

print_status "Using AWS Account ID: ${ACCOUNT_ID}"

# Extract parameters from JSON for validation
if command -v jq &> /dev/null; then
    VPC_ID=$(jq -r '.[] | select(.ParameterKey=="VPCId") | .ParameterValue' "$PARAMETERS_FILE")
    PRIVATE_SUBNET_IDS=$(jq -r '.[] | select(.ParameterKey=="PrivateSubnetIds") | .ParameterValue' "$PARAMETERS_FILE")
    SECURITY_GROUP_IDS=$(jq -r '.[] | select(.ParameterKey=="SecurityGroupIds") | .ParameterValue' "$PARAMETERS_FILE")
    ROUTE_TABLE_ID=$(jq -r '.[] | select(.ParameterKey=="RouteTableId") | .ParameterValue' "$PARAMETERS_FILE")
    ECR_IMAGE=$(jq -r '.[] | select(.ParameterKey=="ECRImage") | .ParameterValue' "$PARAMETERS_FILE")
    
    print_status "ECR Image from parameters: ${ECR_IMAGE}"
else
    print_warning "jq not found. Skipping parameter validation. Install jq for better error detection."
    print_warning "You can install jq with: brew install jq (macOS) or apt-get install jq (Ubuntu)"
    VPC_ID=""
    PRIVATE_SUBNET_IDS=""
    SECURITY_GROUP_IDS=""
    ROUTE_TABLE_ID=""
    ECR_IMAGE=""
fi

# Validate parameters before deployment
print_status "Validating parameters..."
validate_parameters() {
    local validation_errors=()
    
    # Skip validation if jq is not available (parameters are empty)
    if [ -z "$VPC_ID" ]; then
        print_warning "Skipping parameter validation (jq not available)"
        return 0
    fi
    
    # Check if VPC exists
    if ! aws ec2 describe-vpcs --vpc-ids "$VPC_ID" --region "$REGION" > /dev/null 2>&1; then
        validation_errors+=("VPC ID '$VPC_ID' not found in region '$REGION'")
    fi
    
    # Check if subnets exist
    IFS=',' read -ra SUBNET_IDS <<< "$PRIVATE_SUBNET_IDS"
    for subnet_id in "${SUBNET_IDS[@]}"; do
        if ! aws ec2 describe-subnets --subnet-ids "$subnet_id" --region "$REGION" > /dev/null 2>&1; then
            validation_errors+=("Subnet ID '$subnet_id' not found in region '$REGION'")
        fi
    done
    
    # Check if security groups exist
    IFS=',' read -ra SG_IDS <<< "$SECURITY_GROUP_IDS"
    for sg_id in "${SG_IDS[@]}"; do
        if ! aws ec2 describe-security-groups --group-ids "$sg_id" --region "$REGION" > /dev/null 2>&1; then
            validation_errors+=("Security Group ID '$sg_id' not found in region '$REGION'")
        fi
    done
    
    # Check if route table exists
    if ! aws ec2 describe-route-tables --route-table-ids "$ROUTE_TABLE_ID" --region "$REGION" > /dev/null 2>&1; then
        validation_errors+=("Route Table ID '$ROUTE_TABLE_ID' not found in region '$REGION'")
    fi
    
    # Check if ECR repository exists
    if [ -n "$ECR_IMAGE" ]; then
        # Extract repository name from ECR image URI
        # Format: account.dkr.ecr.region.amazonaws.com/repo:tag
        REPO_NAME=$(echo "$ECR_IMAGE" | sed 's|.*/||' | cut -d':' -f1)
        print_status "Validating ECR Repository Name: ${REPO_NAME}"
        if ! aws ecr describe-repositories --repository-names "$REPO_NAME" --region "$REGION" > /dev/null 2>&1; then
            validation_errors+=("ECR repository '$REPO_NAME' not found in region '$REGION'")
        else
            # Check if Docker image exists
            IMAGE_TAG=$(echo "$ECR_IMAGE" | cut -d':' -f2)
            print_status "Validating Docker Image Tag: ${IMAGE_TAG}"
            if ! aws ecr describe-images --repository-name "$REPO_NAME" --image-ids imageTag="$IMAGE_TAG" --region "$REGION" > /dev/null 2>&1; then
                validation_errors+=("Docker image '$IMAGE_TAG' not found in ECR repository '$REPO_NAME'")
            fi
        fi
    fi
    
    # Report validation errors
    if [ ${#validation_errors[@]} -ne 0 ]; then
        print_error "Parameter validation failed:"
        for error in "${validation_errors[@]}"; do
            print_error "  ❌ $error"
        done
        print_error ""
        print_error "Please fix these issues before deploying the stack."
        exit 1
    fi
    
    print_status "Parameter validation passed."
}

# Run validation
validate_parameters

# Check if stack already exists
print_status "Checking if stack '$STACK_NAME' already exists..."
aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" > /dev/null 2>&1
if [ $? -eq 0 ]; then
    print_warning "Stack '$STACK_NAME' already exists."
    read -p "Do you want to update the existing stack? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        print_status "Updating existing stack..."
        OPERATION="update-stack"
        WAIT_OPERATION="stack-update-complete"
    else
        print_status "Exiting. Stack will not be modified."
        rm -f "$TEMP_PARAMETERS_FILE"
        exit 0
    fi
else
    print_status "Creating new stack '$STACK_NAME'..."
    OPERATION="create-stack"
    WAIT_OPERATION="stack-create-complete"
fi

# Create the CloudFormation stack
print_status "Executing CloudFormation $OPERATION..."

STACK_RESULT=$(aws cloudformation $OPERATION \
    --stack-name "$STACK_NAME" \
    --template-body file://"$TEMPLATE_FILE" \
    --parameters file://"$PARAMETERS_FILE" \
    --capabilities CAPABILITY_NAMED_IAM \
    --region "$REGION" 2>&1)

STACK_EXIT_CODE=$?

if [ $STACK_EXIT_CODE -ne 0 ]; then
    print_error "Failed to $OPERATION CloudFormation stack."
    print_error "Error details:"
    echo "$STACK_RESULT"
    
    # Provide root cause analysis
    print_error "Root cause analysis:"
    
    # Check for common error patterns
    if echo "$STACK_RESULT" | grep -q "NoSuchBucket"; then
        print_error "  ❌ S3 bucket not found - Check if the S3 bucket exists and is accessible"
    fi
    
    if echo "$STACK_RESULT" | grep -q "AccessDenied"; then
        print_error "  ❌ Access denied - Check IAM permissions for CloudFormation, ECS, VPC, etc."
    fi
    
    if echo "$STACK_RESULT" | grep -q "InvalidParameterValue"; then
        print_error "  ❌ Invalid parameter value - Check your parameters in parameters.json"
    fi
    
    if echo "$STACK_RESULT" | grep -q "ValidationError"; then
        print_error "  ❌ Template validation error - Check the CloudFormation template syntax"
    fi
    
    if echo "$STACK_RESULT" | grep -q "AlreadyExistsException"; then
        print_error "  ❌ Resource already exists - Some resources may already exist in your account"
    fi
    
    if echo "$STACK_RESULT" | grep -q "LimitExceededException"; then
        print_error "  ❌ AWS service limit exceeded - Check your AWS account limits"
    fi
    
    if echo "$STACK_RESULT" | grep -q "InsufficientCapabilitiesException"; then
        print_error "  ❌ Insufficient capabilities - Add required capabilities to the command"
    fi
    
    if echo "$STACK_RESULT" | grep -q "VPCIdNotFound"; then
        print_error "  ❌ VPC not found - Check if the VPC ID exists in the specified region"
    fi
    
    if echo "$STACK_RESULT" | grep -q "SubnetNotFound"; then
        print_error "  ❌ Subnet not found - Check if the subnet IDs exist and are in the correct VPC"
    fi
    
    if echo "$STACK_RESULT" | grep -q "SecurityGroupNotFound"; then
        print_error "  ❌ Security group not found - Check if the security group IDs exist"
    fi
    
    if echo "$STACK_RESULT" | grep -q "RouteTableNotFound"; then
        print_error "  ❌ Route table not found - Check if the route table ID exists"
    fi
    
    if echo "$STACK_RESULT" | grep -q "RepositoryNotFoundException"; then
        print_error "  ❌ ECR repository not found - Check if the ECR repository exists"
    fi
    
    if echo "$STACK_RESULT" | grep -q "ImageNotFoundException"; then
        print_error "  ❌ Docker image not found - Check if the Docker image exists in ECR"
    fi
    
    # Generic error if no specific pattern matched
    if ! echo "$STACK_RESULT" | grep -q -E "(NoSuchBucket|AccessDenied|InvalidParameterValue|ValidationError|AlreadyExistsException|LimitExceededException|InsufficientCapabilitiesException|VPCIdNotFound|SubnetNotFound|SecurityGroupNotFound|RouteTableNotFound|RepositoryNotFoundException|ImageNotFoundException)"; then
        print_error "  ❌ Unknown error - Check the AWS CloudFormation console for more details"
    fi
    
    print_error ""
    print_error "Troubleshooting steps:"
    print_error "  1. Check the AWS CloudFormation console for detailed error messages"
    print_error "  2. Verify all parameters in parameters.json are correct"
    print_error "  3. Ensure your AWS credentials have sufficient permissions"
    print_error "  4. Check if all referenced resources (VPC, subnets, etc.) exist"
    print_error "  5. Verify the Docker image exists in ECR"
    
    # Ask user if they want to delete the failed stack (if it was created)
    echo ""
    read -p "Do you want to delete the failed stack '$STACK_NAME'? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        print_status "Deleting failed stack '$STACK_NAME'..."
        aws cloudformation delete-stack --stack-name "$STACK_NAME" --region "$REGION"
        
        if [ $? -eq 0 ]; then
            print_status "Stack deletion initiated. Waiting for completion..."
            aws cloudformation wait stack-delete-complete --stack-name "$STACK_NAME" --region "$REGION"
            
            if [ $? -eq 0 ]; then
                print_status "Stack '$STACK_NAME' has been successfully deleted."
            else
                print_error "Stack deletion failed or timed out. Check the CloudFormation console for details."
            fi
        else
            print_error "Failed to initiate stack deletion."
        fi
    else
        print_status "Stack '$STACK_NAME' will remain. You can delete it manually later."
    fi
    
    exit 1
fi

print_status "Stack $OPERATION initiated successfully."

# Wait for stack operation to complete
print_status "Waiting for stack operation to complete..."
aws cloudformation wait $WAIT_OPERATION --stack-name "$STACK_NAME" --region "$REGION"

WAIT_EXIT_CODE=$?

if [ $WAIT_EXIT_CODE -ne 0 ]; then
    print_error "Stack operation failed or timed out."
    print_error ""
    print_error "Analyzing stack events for root cause..."
    
    # Get the latest stack events to identify the failure
    print_error "Latest stack events:"
    aws cloudformation describe-stack-events \
        --stack-name "$STACK_NAME" \
        --region "$REGION" \
        --query 'StackEvents[?ResourceStatus==`CREATE_FAILED` || ResourceStatus==`UPDATE_FAILED` || ResourceStatus==`DELETE_FAILED`].{Resource:LogicalResourceId,Status:ResourceStatus,Reason:ResourceStatusReason}' \
        --output table
    
    print_error ""
    print_error "Root cause analysis from stack events:"
    
    # Get detailed failure information
    FAILED_EVENTS=$(aws cloudformation describe-stack-events \
        --stack-name "$STACK_NAME" \
        --region "$REGION" \
        --query 'StackEvents[?ResourceStatus==`CREATE_FAILED` || ResourceStatus==`UPDATE_FAILED` || ResourceStatus==`DELETE_FAILED`].ResourceStatusReason' \
        --output text)
    
    if echo "$FAILED_EVENTS" | grep -q "VPC.*not found"; then
        print_error "  ❌ VPC not found - Verify VPC ID exists in the specified region"
    fi
    
    if echo "$FAILED_EVENTS" | grep -q "Subnet.*not found"; then
        print_error "  ❌ Subnet not found - Verify subnet IDs exist and are in the correct VPC"
    fi
    
    if echo "$FAILED_EVENTS" | grep -q "SecurityGroup.*not found"; then
        print_error "  ❌ Security group not found - Verify security group ID exists"
    fi
    
    if echo "$FAILED_EVENTS" | grep -q "RouteTable.*not found"; then
        print_error "  ❌ Route table not found - Verify route table ID exists"
    fi
    
    if echo "$FAILED_EVENTS" | grep -q "Repository.*not found"; then
        print_error "  ❌ ECR repository not found - Verify ECR repository exists"
    fi
    
    if echo "$FAILED_EVENTS" | grep -q "Image.*not found"; then
        print_error "  ❌ Docker image not found - Verify Docker image exists in ECR"
    fi
    
    if echo "$FAILED_EVENTS" | grep -q "Access Denied"; then
        print_error "  ❌ Access denied - Check IAM permissions for the failing resource"
    fi
    
    if echo "$FAILED_EVENTS" | grep -q "Limit exceeded"; then
        print_error "  ❌ Service limit exceeded - Check AWS account limits"
    fi
    
    if echo "$FAILED_EVENTS" | grep -q "already exists"; then
        print_error "  ❌ Resource already exists - Some resources may already exist"
    fi
    
    if echo "$FAILED_EVENTS" | grep -q "Invalid parameter"; then
        print_error "  ❌ Invalid parameter - Check parameter values in parameters.json"
    fi
    
    if echo "$FAILED_EVENTS" | grep -q "VPC endpoint"; then
        print_error "  ❌ VPC endpoint issue - Check VPC endpoint configuration"
    fi
    
    if echo "$FAILED_EVENTS" | grep -q "IAM role"; then
        print_error "  ❌ IAM role issue - Check IAM role creation and permissions"
    fi
    
    if echo "$FAILED_EVENTS" | grep -q "ECS"; then
        print_error "  ❌ ECS issue - Check ECS service configuration and task definition"
    fi
    
    print_error ""
    print_error "Troubleshooting steps:"
    print_error "  1. Check the AWS CloudFormation console for detailed error messages"
    print_error "  2. Verify all parameters in parameters.json are correct"
    print_error "  3. Ensure your AWS credentials have sufficient permissions"
    print_error "  4. Check if all referenced resources (VPC, subnets, etc.) exist"
    print_error "  5. Verify the Docker image exists in ECR"
    print_error "  6. Check AWS service limits for your account"
    
    print_error ""
    print_error "To view full stack events:"
    print_error "  aws cloudformation describe-stack-events --stack-name $STACK_NAME --region $REGION"
    
    print_error ""
    print_error "To delete the failed stack:"
    print_error "  aws cloudformation delete-stack --stack-name $STACK_NAME --region $REGION"
    
    # Ask user if they want to delete the failed stack
    echo ""
    read -p "Do you want to delete the failed stack '$STACK_NAME'? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        print_status "Deleting failed stack '$STACK_NAME'..."
        aws cloudformation delete-stack --stack-name "$STACK_NAME" --region "$REGION"
        
        if [ $? -eq 0 ]; then
            print_status "Stack deletion initiated. Waiting for completion..."
            aws cloudformation wait stack-delete-complete --stack-name "$STACK_NAME" --region "$REGION"
            
            if [ $? -eq 0 ]; then
                print_status "Stack '$STACK_NAME' has been successfully deleted."
            else
                print_error "Stack deletion failed or timed out. Check the CloudFormation console for details."
            fi
        else
            print_error "Failed to initiate stack deletion."
        fi
    else
        print_status "Stack '$STACK_NAME' will remain. You can delete it manually later."
    fi
    
    exit 1
fi

# Get stack outputs
print_status "Getting stack outputs..."
aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --region "$REGION" \
    --query 'Stacks[0].Outputs' \
    --output table

print_status "Stack operation completed successfully!"
print_status "Stack Name: $STACK_NAME"
print_status "Region: $REGION"

# Show useful commands
echo ""
print_status "Useful commands:"
echo "  # View stack status:"
echo "  aws cloudformation describe-stacks --stack-name $STACK_NAME --region $REGION"
echo ""
echo "  # View stack events:"
echo "  aws cloudformation describe-stack-events --stack-name $STACK_NAME --region $REGION"
echo ""
echo "  # Delete stack:"
echo "  aws cloudformation delete-stack --stack-name $STACK_NAME --region $REGION"
echo ""
echo "  # View ECS service:"
echo "  aws ecs describe-services --cluster $STACK_NAME-kcl-keyspaces-cluster --services $STACK_NAME-kcl-keyspaces-runner --region $REGION"
echo ""
echo "  # View CloudWatch logs:"
echo "  aws logs tail /ecs/$STACK_NAME-kcl-keyspaces-runner --region $REGION --follow"
