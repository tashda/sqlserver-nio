import Foundation

/// SQL Server query splitter for handling GO delimiters and batch separation
public struct SQLServerQuerySplitter {
    
    public struct SplitterOptions {
        public let allowGoDelimiter: Bool
        public let adaptiveGoSplit: Bool
        public let allowSemicolon: Bool
        public let ignoreComments: Bool
        public let preventSingleLineSplit: Bool
        
        public init(
            allowGoDelimiter: Bool = true,
            adaptiveGoSplit: Bool = false,
            allowSemicolon: Bool = false,
            ignoreComments: Bool = false,
            preventSingleLineSplit: Bool = false
        ) {
            self.allowGoDelimiter = allowGoDelimiter
            self.adaptiveGoSplit = adaptiveGoSplit
            self.allowSemicolon = allowSemicolon
            self.ignoreComments = ignoreComments
            self.preventSingleLineSplit = preventSingleLineSplit
        }
        
        /// MSSQL splitter options for standard SQL Server batch processing
        public static let mssql = SplitterOptions(
            allowGoDelimiter: true,
            adaptiveGoSplit: false,
            allowSemicolon: false
        )
        
        /// MSSQL editor options for interactive query execution
        public static let mssqlEditor = SplitterOptions(
            allowGoDelimiter: true,
            adaptiveGoSplit: true,
            allowSemicolon: false,
            ignoreComments: true,
            preventSingleLineSplit: true
        )
    }
    
    public struct SplitResult {
        public let text: String
        public let startPosition: Int
        public let endPosition: Int
        
        public init(text: String, startPosition: Int, endPosition: Int) {
            self.text = text
            self.startPosition = startPosition
            self.endPosition = endPosition
        }
    }
    
    /// Splits SQL text into batches using SQL Server batch separation rules
    public static func splitQuery(_ sql: String, options: SplitterOptions = .mssql) -> [SplitResult] {
        var results: [SplitResult] = []
        var currentBatch = ""
        var batchStartPosition = 0
        var position = 0
        var wasDataOnLine = false
        var currentDelimiter: String? = options.allowSemicolon ? ";" : nil
        
        if options.adaptiveGoSplit {
            currentDelimiter = ";"
        }
        
        let source = sql
        let end = source.count
        
        while position < end {
            let startIndex = source.index(source.startIndex, offsetBy: position)
            let char = source[startIndex]
            
            // Handle newlines
            if char == "\n" {
                currentBatch.append(char)
                position += 1
                wasDataOnLine = false
                continue
            }
            
            // Handle whitespace
            if char == " " || char == "\t" || char == "\r" {
                currentBatch.append(char)
                position += 1
                continue
            }
            
            // Check for GO delimiter at start of line
            if (options.allowGoDelimiter || options.adaptiveGoSplit) && !wasDataOnLine {
                let remainingString = String(source[startIndex...])
                
                // Match GO followed by optional whitespace and newline/end
                if let range = remainingString.range(of: #"^GO[\t\r ]*(\n|$)"#, options: [.regularExpression, .caseInsensitive]) {
                    // Found GO delimiter
                    let matchLength = remainingString.distance(from: remainingString.startIndex, to: range.upperBound)
                    
                    // Add current batch if not empty
                    let trimmedBatch = currentBatch.trimmingCharacters(in: .whitespacesAndNewlines)
                    if !trimmedBatch.isEmpty {
                        results.append(SplitResult(
                            text: trimmedBatch,
                            startPosition: batchStartPosition,
                            endPosition: position
                        ))
                    }
                    
                    // Start new batch
                    currentBatch = ""
                    batchStartPosition = position + matchLength
                    
                    // Skip the GO delimiter
                    position += matchLength
                    wasDataOnLine = false
                    
                    if options.adaptiveGoSplit {
                        currentDelimiter = ";"
                    }
                    continue
                }
            }
            
            // Check for CREATE PROCEDURE/FUNCTION/TRIGGER (adaptive GO split)
            if options.adaptiveGoSplit && !wasDataOnLine {
                let remainingString = String(source[startIndex...])
                if let _ = remainingString.range(of: #"^(CREATE|ALTER)\s*(PROCEDURE|FUNCTION|TRIGGER)"#, options: [.regularExpression, .caseInsensitive]) {
                    currentDelimiter = nil // Switch to GO-only mode
                }
            }
            
            // Check for semicolon delimiter
            if let delimiter = currentDelimiter, char == Character(delimiter) {
                if !options.preventSingleLineSplit || !containsDataAfterDelimiterOnLine(source, position: position) {
                    // Add current batch
                    currentBatch.append(char)
                    let trimmedBatch = currentBatch.trimmingCharacters(in: .whitespacesAndNewlines)
                    if !trimmedBatch.isEmpty {
                        results.append(SplitResult(
                            text: trimmedBatch,
                            startPosition: batchStartPosition,
                            endPosition: position + 1
                        ))
                    }
                    
                    // Start new batch
                    currentBatch = ""
                    batchStartPosition = position + 1
                    position += 1
                    continue
                }
            }
            
            // Regular character
            currentBatch.append(char)
            position += 1
            if char != " " && char != "\t" && char != "\r" {
                wasDataOnLine = true
            }
        }
        
        // Add final batch if not empty
        let trimmedBatch = currentBatch.trimmingCharacters(in: .whitespacesAndNewlines)
        if !trimmedBatch.isEmpty {
            results.append(SplitResult(
                text: trimmedBatch,
                startPosition: batchStartPosition,
                endPosition: position
            ))
        }
        
        return results
    }
    
    /// Checks if there's data after delimiter on the same line
    private static func containsDataAfterDelimiterOnLine(_ source: String, position: Int) -> Bool {
        var pos = position + 1
        let end = source.count
        
        while pos < end {
            let index = source.index(source.startIndex, offsetBy: pos)
            let char = source[index]
            
            if char == "\n" {
                return false
            }
            if char != " " && char != "\t" && char != "\r" {
                return true
            }
            pos += 1
        }
        
        return false
    }
}