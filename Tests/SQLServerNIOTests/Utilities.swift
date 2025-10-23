import Foundation
import Logging
import NIO
import SQLServerNIO
import XCTest

func env(_ name: String) -> String? {
    getenv(name).flatMap { String(cString: $0) }
}

private var hasLoadedEnvironmentFile = false

func loadEnvFileIfPresent(path: String = ".env") {
    guard !hasLoadedEnvironmentFile else { return }
    hasLoadedEnvironmentFile = true
    
    guard FileManager.default.fileExists(atPath: path) else { return }
    
    guard let contents = try? String(contentsOfFile: path, encoding: .utf8) else {
        return
    }
    
    let newlineSet = CharacterSet.newlines
    let whitespaceSet = CharacterSet.whitespacesAndNewlines
    
    contents.components(separatedBy: newlineSet).forEach { rawLine in
        let line = rawLine.trimmingCharacters(in: whitespaceSet)
        guard !line.isEmpty, !line.hasPrefix("#") else { return }
        
        guard let separatorIndex = line.firstIndex(of: "=") else { return }
        
        let keySubstring = line[..<separatorIndex]
        let valueSubstring = line[line.index(after: separatorIndex)...]
        
        let trimmedKey = keySubstring.trimmingCharacters(in: whitespaceSet)
        var trimmedValue = valueSubstring.trimmingCharacters(in: whitespaceSet)
        
        if trimmedValue.hasPrefix("\"") && trimmedValue.hasSuffix("\""), trimmedValue.count >= 2 {
            trimmedValue = String(trimmedValue.dropFirst().dropLast())
        } else if trimmedValue.hasPrefix("'") && trimmedValue.hasSuffix("'"), trimmedValue.count >= 2 {
            trimmedValue = String(trimmedValue.dropFirst().dropLast())
        }
        
        let key = String(trimmedKey)
        let value = String(trimmedValue)
        
        guard !key.isEmpty else { return }
        setenv(key, value, 1)
    }
}

func makeTempTableName(prefix: String = "tmp") -> String {
    let token = UUID().uuidString.replacingOccurrences(of: "-", with: "")
    return "#\(prefix)_\(token)"
}

func makeSchemaQualifiedName(prefix: String, schema: String = "dbo") -> (bare: String, bracketed: String, nameOnly: String) {
    let token = UUID().uuidString.replacingOccurrences(of: "-", with: "")
    let name = "\(prefix)_\(token)"
    let bare = "\(schema).\(name)"
    let bracketed = "[\(schema)].[\(name)]"
    return (bare, bracketed, name)
}

func requireEnvFlag(_ key: String, description: String) throws {
    guard env(key) == "1" else {
        throw XCTSkip("Skipping \(description). Set \(key)=1 to enable.")
    }
}

func makeSQLServerConnectionConfiguration() -> SQLServerConnection.Configuration {
    let hostname = env("TDS_HOSTNAME") ?? "localhost"
    let port = env("TDS_PORT").flatMap(Int.init) ?? 1433
    let username = env("TDS_USERNAME") ?? "swift_tds_user"
    let password = env("TDS_PASSWORD") ?? "SwiftTDS!"
    let database = env("TDS_DATABASE") ?? "swift_tds_database"

    return SQLServerConnection.Configuration(
        hostname: hostname,
        port: port,
        login: .init(
            database: database,
            authentication: .sqlPassword(username: username, password: password)
        ),
        tlsConfiguration: nil,
        metadataConfiguration: SQLServerMetadataClient.Configuration(
            includeSystemSchemas: false,
            enableColumnCache: true
        ),
        retryConfiguration: SQLServerRetryConfiguration()
    )
}

func makeSQLServerClientConfiguration() -> SQLServerClient.Configuration {
    let pool = SQLServerConnectionPool.Configuration(
        maximumConcurrentConnections: 4,
        minimumIdleConnections: 1,
        connectionIdleTimeout: .seconds(60),
        validationQuery: "SELECT 1;"
    )

    return SQLServerClient.Configuration(
        connection: makeSQLServerConnectionConfiguration(),
        poolConfiguration: pool
    )
}

func connectSQLServer(on eventLoop: EventLoop) -> EventLoopFuture<SQLServerConnection> {
    SQLServerConnection.connect(configuration: makeSQLServerConnectionConfiguration(), on: eventLoop)
}

extension NSRegularExpression {
    convenience init(_ pattern: String) {
        do {
            try self.init(pattern: pattern)
        } catch {
            preconditionFailure("Illegal regular expression: \(pattern).")
        }
    }
    
    func matches(_ string: String?) -> Bool {
        guard let str = string else { return false }
        let range = NSRange(location: 0, length: str.utf16.count)
        return firstMatch(in: str, options: [], range: range) != nil
    }
}

let sqlServerVersionPattern = "[0-9]{2}\\.[0-9]{1}\\.[0-9]{4}\\.[0-9]{1}"

enum TestTimeoutError: Error, LocalizedError {
    case timedOut(timeout: TimeInterval, description: String)

    var errorDescription: String? {
        switch self {
        case .timedOut(let timeout, let description):
            return "Operation '\(description)' timed out after \(timeout) seconds"
        }
    }
}

extension XCTestCase {
    func waitForResult<T>(
        _ future: EventLoopFuture<T>,
        timeout: TimeInterval,
        description: String,
        file: StaticString = #filePath,
        line: UInt = #line
    ) throws -> T {
        let expectation = expectation(description: description)
        var result: Result<T, Error>?

        future.whenComplete { value in
            result = value
            expectation.fulfill()
        }

        let waiterResult = XCTWaiter.wait(for: [expectation], timeout: timeout)
        guard waiterResult == .completed else {
            XCTFail("Operation '\(description)' did not complete within \(timeout) seconds", file: file, line: line)
            throw TestTimeoutError.timedOut(timeout: timeout, description: description)
        }

        guard let resolved = result else {
            XCTFail("Operation '\(description)' completed without result", file: file, line: line)
            throw TestTimeoutError.timedOut(timeout: timeout, description: description)
        }

        switch resolved {
        case .success(let value):
            return value
        case .failure(let error):
            throw error
        }
    }
}
