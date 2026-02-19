#!/bin/bash

# Build Lambda layers compatible with Python 3.10 runtime
# This script uses Docker with Amazon Linux 2 to ensure glibc compatibility
#
# USAGE:
#   ./build_lambda_layers.sh <layer_name> [requirements_file_or_package_list]
#
# EXAMPLES:
#   # Build from requirements.txt file
#   ./build_lambda_layers.sh my-layer requirements.txt
#
#   # Build from CLI package list (space-separated)
#   ./build_lambda_layers.sh beautifulsoup4-layer beautifulsoup4==4.12.3 requests==2.32.5
#
#   # Build single package
#   ./build_lambda_layers.sh requests-layer requests==2.32.5

set -e

# Validate arguments
if [ $# -lt 2 ]; then
    echo "Usage: $0 <layer_name> [requirements_file_or_package_list]"
    echo ""
    echo "Examples:"
    echo "  $0 my-layer requirements.txt"
    echo "  $0 beautifulsoup4-layer beautifulsoup4==4.12.3 requests==2.32.5"
    echo "  $0 requests-layer requests==2.32.5"
    exit 1
fi

LAYER_NAME=$1
shift  # Remove layer_name from args
REQUIREMENT_INPUT=("$@")  # Everything else is either a file or package list

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"
LAYERS_DIR="$PROJECT_ROOT/lambda_layers"

echo "=========================================="
echo "Building Lambda Layer: $LAYER_NAME"
echo "Python: 3.10"
echo "=========================================="

# Create layers directory
mkdir -p "$LAYERS_DIR"

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed or not in PATH"
    echo "Please install Docker to build Lambda layers"
    exit 1
fi

# Determine if input is a requirements file or package list
TEMP_REQUIREMENTS_FILE=""
CLEANUP_TEMP=false

if [ ${#REQUIREMENT_INPUT[@]} -eq 1 ] && [ -f "${REQUIREMENT_INPUT[0]}" ]; then
    # Input is a requirements file
    REQUIREMENTS_FILE="${REQUIREMENT_INPUT[0]}"
    echo "Using requirements file: $REQUIREMENTS_FILE"
    if [ ! -f "$REQUIREMENTS_FILE" ]; then
        echo "❌ Error: Requirements file not found: $REQUIREMENTS_FILE"
        exit 1
    fi
elif [ ${#REQUIREMENT_INPUT[@]} -ge 1 ]; then
    # Input is a list of packages
    TEMP_REQUIREMENTS_FILE="$LAYERS_DIR/${LAYER_NAME}_temp_requirements.txt"
    echo "Creating temporary requirements file from package list..."
    
    # Create requirements file from package arguments
    > "$TEMP_REQUIREMENTS_FILE"  # Clear file
    for package in "${REQUIREMENT_INPUT[@]}"; do
        echo "$package" >> "$TEMP_REQUIREMENTS_FILE"
    done
    
    REQUIREMENTS_FILE="$TEMP_REQUIREMENTS_FILE"
    CLEANUP_TEMP=true
    
    echo "Packages to install:"
    cat "$REQUIREMENTS_FILE" | sed 's/^/  - /'
else
    echo "❌ Error: No requirements file or package list provided"
    exit 1
fi

# Build the layer
build_layer() {
    local layer_name=$1
    local requirements_file=$2
    local layer_dir="$LAYERS_DIR/$layer_name"
    
    echo ""
    echo "Building layer: $layer_name"
    
    # Clean up previous build
    rm -rf "$layer_dir"
    mkdir -p "$layer_dir"
    
    # Use Docker to build with correct glibc (Amazon Linux 2 matching Lambda runtime)
    echo "Installing packages in Docker container (amazon/aws-lambda-python:3.10)..."
    docker run --rm \
        --entrypoint "" \
        -v "$PROJECT_ROOT:$PROJECT_ROOT" \
        -w "$layer_dir" \
        amazon/aws-lambda-python:3.10 \
        bash -c "pip install -r $requirements_file -t python/lib/python3.10/site-packages/ --no-cache-dir"
    
    # Create zip file
    echo "Creating deployment package..."
    cd "$layer_dir"
    zip -r -q "../${layer_name}.zip" . 
    cd - > /dev/null
    
    # Get file size
    LAYER_SIZE=$(du -h "$LAYERS_DIR/${layer_name}.zip" | cut -f1)
    
    echo "✓ Created layer: ${layer_name}.zip (${LAYER_SIZE})"
}

# Execute build
build_layer "$LAYER_NAME" "$REQUIREMENTS_FILE"

# Cleanup temporary requirements file if created
if [ "$CLEANUP_TEMP" = true ] && [ -f "$TEMP_REQUIREMENTS_FILE" ]; then
    rm -f "$TEMP_REQUIREMENTS_FILE"
    echo "✓ Cleaned up temporary requirements file"
fi

echo ""
echo "=========================================="
echo "✓ Layer build completed!"
echo "=========================================="
echo ""
echo "Next step: Publish layer to AWS Lambda"
echo ""
echo "Command:"
echo ""
echo "  export AWS_REGION=us-east-1"
echo "  export AWS_ACCOUNT_ID=\$(aws sts get-caller-identity --query Account --output text)"
echo ""
echo "  aws lambda publish-layer-version \\"
echo "    --layer-name $LAYER_NAME \\"
echo "    --zip-file fileb://$LAYERS_DIR/${LAYER_NAME}.zip \\"
echo "    --compatible-runtimes python3.10 \\"
echo "    --region \$AWS_REGION"
echo ""
echo "Then update serverless.yml with the returned LayerVersionArn"
echo ""
