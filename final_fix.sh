#!/bin/bash

echo "🔧 Final comprehensive fix for SQL Server tests..."

# Find all test files that reference client or group but don't have these properties
echo "   Finding files needing client/group properties..."

files_to_fix=()

while IFS= read -r file; do
    # Check if file references self.client or self.group
    if grep -q "self\.client\|self\.group" "$file" 2>/dev/null; then
        # Check if the file has the properties declared
        if ! grep -q "var group:" "$file" 2>/dev/null || ! grep -q "var client:" "$file" 2>/dev/null; then
            files_to_fix+=("$file")
        fi
    fi
done < <(find Tests -name "*.swift" -type f)

# Add the standard test infrastructure to files that need it
for file in "${files_to_fix[@]}"; do
    echo "      Fixing $(basename "$file")..."

    # Find the class declaration
    class_line=$(grep -n "final class.*: XCTestCase" "$file" | head -1 | cut -d: -f1)

    if [ -n "$class_line" ]; then
        # Insert the properties after the class declaration
        sed -i '' "${class_line}a\\
    var group: EventLoopGroup!\\
    var client: SQLServerClient!\\
" "$file"
    fi
done

# Add missing imports for centralized functions
echo "   Adding missing imports..."

find Tests -name "*.swift" -exec grep -l "withTimeout\|withRetry\|withTemporaryDatabase" {} \; | while read file; do
    if ! grep -q "makeSQLServerClientConfiguration\|TestEnvironmentManager" "$file"; then
        echo "      Adding infrastructure imports to $(basename "$file")"
        # Add import after the existing imports
        sed -i '' '/^@testable import SQLServerKit$/a\
import Foundation
' "$file"
    fi
done

echo "✅ Final fixes completed!"