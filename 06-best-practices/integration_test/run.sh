#!/usr/bin/env bash

# Navigate to the script's directory
if [[ -z "${GITHUB_ACTIONS}" ]]; then
  cd "$(dirname "$0")"
fi

# Build the Docker image if LOCAL_IMAGE_NAME is not set
if [ "${LOCAL_IMAGE_NAME}" == "" ]; then 
    LOCAL_TAG=$(date +"%Y-%m-%d-%H-%M")
    export LOCAL_IMAGE_NAME="integration-test-image:${LOCAL_TAG}"
    echo "LOCAL_IMAGE_NAME is not set, building a new image with tag ${LOCAL_IMAGE_NAME}"
    docker build -t ${LOCAL_IMAGE_NAME} ..
else
    echo "No need to build image ${LOCAL_IMAGE_NAME}"
fi

# Start Docker Compose services
docker-compose up -d

# Wait for Localstack to initialize
sleep 10

# Create the S3 bucket in Localstack
aws --endpoint-url=http://localhost:4566 \
    s3 mb s3://nyc-duration

# Run the integration test
docker run --rm -e S3_ENDPOINT_URL=http://localstack:4566 \
    --network="host" \
    ${LOCAL_IMAGE_NAME} python tests/integration_test.py

# Capture the exit code
ERROR_CODE=$?

if [ ${ERROR_CODE} != 0 ]; then
    echo "Integration test failed. Logs:"
    docker-compose logs
    docker-compose down
    exit ${ERROR_CODE}
fi

echo "Integration test completed successfully."

# Stop Docker Compose services
docker-compose down
