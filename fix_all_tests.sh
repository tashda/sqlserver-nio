#!/bin/bash

echo "🔧 Fixing all SQL Server test files..."

# Replace all instances of SQLServerTestCase with XCTestCase
echo "   Replacing SQLServerTestCase..."
find Tests -name "*.swift" -exec sed -i '' 's/: SQLServerTestCase/: XCTestCase/g' {} \;

# Replace common problematic function calls with shared infrastructure equivalents
echo "   Fixing loadEnvFileIfPresent calls..."
find Tests -name "*.swift" -exec sed -i '' 's/loadEnvFileIfPresent()/TestEnvironmentManager.loadEnvironmentVariables(); \/\/ Load environment configuration/g' {} \;

echo "   Fixing requireEnvFlag calls..."
find Tests -name "*.swift" -exec sed -i '' 's/requireEnvFlag("\(.*\)", description: "\(.*\)")/if !envFlagEnabled("\1") { throw XCTSkip("Enable \1=1 to run \2") }/g' {} \;

# Add missing imports for test infrastructure
echo "   Adding imports..."
for file in $(find Tests -name "*.swift"); do
    if ! grep -q "TestEnvironmentManager.loadEnvironmentVariables" "$file" && grep -q "loadEnvFileIfPresent\|requireEnvFlag" "$file"; then
        echo "      Adding environment management to $(basename $file)"
        sed -i '' 's/@testable import SQLServerKit/@testable import SQLServerKit\
import Foundation/' "$file"
    fi
done

# Fix common async setup patterns
echo "   Fixing async setup patterns..."
find Tests -name "*.swift" -exec sed -i '' 's/override func setUp() {/override func setUp() async throws {\
        continueAfterFailure = false\
\
        \/\/ Load environment configuration\
        TestEnvironmentManager.loadEnvironmentVariables()\
\
        \/\/ Configure logging\
        _ = isLoggingConfigured/g' {} \;

# Clean up any duplicate lines
find Tests -name "*.swift" -exec sed -i '' '/^@testable import SQLServerKit$/N; /^\@testable import SQLServerKit$/{ s/^@testable import SQLServerKit$/@testable import SQLServerKit/; t; d; }' {} \;

echo "✅ Test file fixes completed!"