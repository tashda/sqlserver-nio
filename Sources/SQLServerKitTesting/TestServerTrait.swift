import Foundation
import Testing

/// Gives a suite or test the server named by a URL variable as `TestServer.current`.
///
/// ```swift
/// @Suite(.testServer)
/// struct QueryTests {
///     @Test func selectsOne() async throws {
///         let server = try #require(TestServer.current)
///         let connection = try await SQLServerConnection.connect(configuration: server.configuration)
///     }
/// }
///
/// @Suite(.testServer("SQLSERVER_TEST_TLS_URL"))
/// struct CertificateTests { … }
/// ```
///
/// Without the variable the suite is skipped and the skip names it; with
/// `SQLSERVER_TEST_REQUIRED=1` it fails instead. A URL that cannot be read always fails.
public struct TestServerTrait: SuiteTrait, TestTrait, TestScoping {
    public let variable: String

    public var isRecursive: Bool { true }

    public func prepare(for test: Test) async throws {
        switch TestServer.availability(of: variable) {
        case .available:
            return
        case .fail(let message):
            throw TestServer.Unavailable(message)
        case .skip(let message):
            // A ConditionTrait that does not hold skips the test with its comment.
            try await ConditionTrait.disabled(Comment(rawValue: message)).prepare(for: test)
        }
    }

    public func scopeProvider(for test: Test, testCase: Test.Case?) -> TestServerTrait? {
        // Once for a suite, and once for each test case.
        test.isSuite || testCase != nil ? self : nil
    }

    public func provideScope(
        for test: Test,
        testCase: Test.Case?,
        performing function: @Sendable () async throws -> Void
    ) async throws {
        guard let server = try TestServer.load(variable) else { throw TestServer.Unavailable(TestServer.missingMessage(variable)) }
        try await TestServer.$current.withValue(server) { try await function() }
    }
}

extension Trait where Self == TestServerTrait {
    /// The server in `SQLSERVER_TEST_URL`.
    public static var testServer: Self { TestServerTrait(variable: TestServer.defaultVariable) }

    /// The server in `variable`, for example `SQLSERVER_TEST_TLS_URL`.
    public static func testServer(_ variable: String) -> Self { TestServerTrait(variable: variable) }
}
