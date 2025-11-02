#!/bin/bash

# Script to run tests with environment variables that work with Xcode
# Usage: ./run-xcode-tests.sh [test_filter] [additional swift test args]

TEST_FILTER="$1"
shift

# Load environment variables from .env file as fallback
if [ -f ".env" ]; then
    echo "Loading fallback environment from .env file..."
    export $(grep -v '^#' .env | xargs)
fi

# Display current environment variables (for debugging)
echo "🔍 Current Environment Variables:"
echo "   TDS_HOSTNAME: ${TDS_HOSTNAME:-<not set>}"
echo "   TDS_PORT: ${TDS_PORT:-<not set>}"
echo "   TDS_DATABASE: ${TDS_DATABASE:-<not set>}"
echo "   TDS_USERNAME: ${TDS_USERNAME:-<not set>}"
echo "   TDS_PASSWORD: ${TDS_PASSWORD:+<set>}"
echo ""

# Run the tests
if [ -n "$TEST_FILTER" ]; then
    echo "Running tests with filter: $TEST_FILTER"
    swift test --filter "$TEST_FILTER" "$@"
else
    echo "Running all tests"
    swift test "$@"
fi