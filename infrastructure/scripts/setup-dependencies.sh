#!/bin/bash

# Setup script for Keyspaces CDC Streams to S3 project
# This script builds the AWS Keyspaces Streams Kinesis Adapter locally and verifies dependencies

set -e

ADAPTER_REPO="https://github.com/aws/keyspaces-streams-kinesis-adapter.git"
ADAPTER_DIR="keyspaces-streams-kinesis-adapter"

ADAPTER_VERSION="1.0.0"

echo "🔧 Setting up dependencies for Keyspaces CDC Streams to S3..."

# Check prerequisites
echo "🔍 Checking prerequisites..."

# Check Java version
if ! command -v java &> /dev/null; then
    echo "❌ Java is not installed. Please install Java 8 or higher."
    exit 1
fi

JAVA_VERSION=$(java -version 2>&1 | head -n1 | cut -d'"' -f2 | cut -d'.' -f1-2)
echo "✅ Java version: $JAVA_VERSION"

# Check Maven
if ! command -v mvn &> /dev/null; then
    echo "❌ Maven is not installed. Please install Maven 3.6+."
    exit 1
fi

MVN_VERSION=$(mvn -version | head -n1 | cut -d' ' -f3)
echo "✅ Maven version: $MVN_VERSION"

# Check Git
if ! command -v git &> /dev/null; then
    echo "❌ Git is not installed. Please install Git."
    exit 1
fi

echo "✅ All prerequisites are available"

# Function to get Maven local repository path (cross-platform)
get_maven_repo_path() {
    # Try to get from Maven settings first (suppress warnings)
    local maven_repo
    maven_repo=$(mvn help:evaluate -Dexpression=settings.localRepository -q -DforceStdout 2>/dev/null | grep -v "WARNING" | head -n1 || true)
    
    # Clean up any remaining output and check if valid
    maven_repo=$(echo "$maven_repo" | tr -d '\r\n' | xargs)
    
    # Fallback to default locations if Maven command fails or returns invalid path
    if [ -z "$maven_repo" ] || [ "$maven_repo" = "null" ] || [[ "$maven_repo" == *"WARNING"* ]]; then
        if [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "cygwin" ]] || [[ "$OSTYPE" == "win32" ]]; then
            # Windows
            maven_repo="${USERPROFILE:-$HOME}/.m2/repository"
        else
            # Unix-like (Linux, macOS, etc.)
            maven_repo="$HOME/.m2/repository"
        fi
    fi
    
    echo "$maven_repo"
}

# Function to check if adapter is already installed in local Maven repository
check_adapter_installed() {
    local maven_repo
    maven_repo=$(get_maven_repo_path)
    local adapter_path="$maven_repo/software/amazon/keyspaces/keyspaces-streams-kinesis-adapter/$ADAPTER_VERSION"
    if [ -d "$adapter_path" ] && [ -f "$adapter_path/keyspaces-streams-kinesis-adapter-$ADAPTER_VERSION.jar" ]; then
        return 0
    else
        return 1
    fi
}

# Test if the project can build successfully with existing dependencies
echo "🔍 Testing project build with existing dependencies..."
if mvn clean package -DskipTests -q 2>/dev/null; then
    echo "✅ Project builds successfully!"
    echo "✅ AWS Keyspaces Streams Kinesis Adapter is already available"
    echo ""
    echo "🎉 Your project is ready to use!"
    echo ""
    echo "📋 Next steps:"
    echo "  • Build: mvn clean package"
    echo "  • Deploy: ./infrastructure/scripts/deploy.sh"
    echo ""
    exit 0
fi

echo "❌ Project build failed. Need to build Keyspaces Streams Kinesis Adapter locally."

# Check if adapter is already built and installed
if check_adapter_installed; then
    echo "✅ Keyspaces Streams Kinesis Adapter is already installed in local Maven repository"
    echo "🔍 Testing project build again..."
    if mvn clean package -DskipTests -q; then
        echo "✅ Project builds successfully!"
        echo ""
        echo "🎉 Your project is ready to use!"
        echo ""
        echo "📋 Next steps:"
        echo "  • Build: mvn clean package"
        echo "  • Deploy: ./infrastructure/scripts/deploy.sh"
        echo ""
        exit 0
    else
        echo "❌ Project still fails to build. There might be other issues."
        echo "Please check the error messages above."
        exit 1
    fi
fi

# Clone and build the Keyspaces Streams Kinesis Adapter
echo ""
echo "🚀 Building AWS Keyspaces Streams Kinesis Adapter locally..."

# Clean up any existing directory
if [ -d "$ADAPTER_DIR" ]; then
    echo "🧹 Cleaning up existing adapter directory..."
    rm -rf "$ADAPTER_DIR"
fi

# Clone the repository
echo "📥 Cloning Keyspaces Streams Kinesis Adapter repository..."
if ! git clone "$ADAPTER_REPO" "$ADAPTER_DIR"; then
    echo "❌ Failed to clone the repository. Please check your internet connection."
    exit 1
fi

# Build and install the adapter
echo "🔨 Building and installing the adapter to local Maven repository..."
cd "$ADAPTER_DIR"

if ! mvn clean install -DskipTests; then
    echo "❌ Failed to build the Keyspaces Streams Kinesis Adapter."
    echo "Please check the error messages above."
    cd ..
    exit 1
fi

cd ..

echo "✅ Successfully built and installed Keyspaces Streams Kinesis Adapter"

# Clean up the cloned directory
echo "🧹 Cleaning up temporary files..."
rm -rf "$ADAPTER_DIR"

# Verify the adapter is now available
if check_adapter_installed; then
    echo "✅ Keyspaces Streams Kinesis Adapter is now available in local Maven repository"
else
    echo "❌ Failed to install the adapter to local Maven repository"
    exit 1
fi

# Test the project build again
echo "🔍 Testing project build with newly installed adapter..."
if mvn clean package -DskipTests -q; then
    echo "✅ Project builds successfully!"
    echo ""
    echo "🎉 Setup completed successfully!"
    echo ""
    echo "📋 What was done:"
    echo "  • Cloned AWS Keyspaces Streams Kinesis Adapter from GitHub"
    echo "  • Built and installed it to your local Maven repository (~/.m2/repository/)"
    echo "  • Verified your project can now build successfully"
    echo ""
    echo "📋 Next steps:"
    echo "  • Build: mvn clean package"
    echo "  • Deploy: ./infrastructure/scripts/deploy.sh"
    echo ""
else
    echo "❌ Project still fails to build after installing the adapter."
    echo "There might be other compilation issues. Please check the error messages above."
    exit 1
fi
