#!/bin/bash

# Load environment variables from .env file
if [ -f ".env" ]; then
    export $(grep -v '^#' .env | xargs)
    echo "Environment variables loaded from .env:"
    echo "TDS_HOSTNAME=$TDS_HOSTNAME"
    echo "TDS_PORT=$TDS_PORT"
    echo "TDS_DATABASE=$TDS_DATABASE"
    echo "TDS_USERNAME=$TDS_USERNAME"
    echo "TDS_PASSWORD=***"
else
    echo "Error: .env file not found"
    exit 1
fi

echo ""
echo "Running tests..."
echo ""

# Run the tests with environment variables
swift test "$@"