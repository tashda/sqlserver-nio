import XCTest
import NIOCore
@testable import SQLServerKit
@testable import SQLServerTDS

final class ErrorAndConfigurationContractTests: XCTestCase {
    private func message(_ number: Int32, _ kind: SQLServerStreamMessage.Kind = .error, severity: UInt8 = 16) -> SQLServerStreamMessage {
        SQLServerStreamMessage(kind: kind, number: number, message: "m\(number)", state: 1, severity: severity, serverName: "s", procedureName: "", lineNumber: 1)
    }

    func testFirstServerErrorBecomesPrimaryAndAllMessagesAreKept() throws {
        let error = try XCTUnwrap(SQLServerError.fromServerMessages([message(0, .info), message(547), message(3621)]))
        guard case .sqlExecutionError(let text, let details?) = error else { return XCTFail("\(error)") }
        XCTAssertEqual(text, "m547")
        XCTAssertEqual(details.number, 547)
        XCTAssertEqual(details.messages.count, 3)
        XCTAssertEqual(details.errors.map(\.number), [547, 3621])
        XCTAssertFalse(error.isTransient)
        XCTAssertFalse(error.isConnectionLost)
    }

    func testInformationalMessagesAloneAreNotErrors() {
        XCTAssertNil(SQLServerError.fromServerMessages([message(5701, .info, severity: 0)]))
    }

    func testDeadlockVictimIsClassifiedEvenWhenNotFirst() throws {
        let error = try XCTUnwrap(SQLServerError.fromServerMessages([message(3998), message(1205, severity: 13)]))
        guard case .deadlockDetected = error else { return XCTFail("\(error)") }
        XCTAssertTrue(error.isTransient)
        XCTAssertEqual(error.serverErrorNumber, 3998)
    }

    func testFatalSeverityMeansConnectionLost() throws {
        let error = try XCTUnwrap(SQLServerError.fromServerMessages([message(4014, severity: 20)]))
        XCTAssertTrue(error.isConnectionLost)
    }

    func testCancellationIsNotAConnectionLoss() {
        XCTAssertFalse(SQLServerError.protocolError(.cancelled).isConnectionLost)
        XCTAssertTrue(SQLServerError.protocolError(.protocolError("desync")).isConnectionLost)
        guard case .timeout = SQLServerError.normalize(TDSError.requestTimeout("t")) else {
            return XCTFail("Request timeouts normalize to SQLServerError.timeout")
        }
    }

    func testOptionalEncryptionWithoutConfigurationStillEncrypts() throws {
        let login = SQLServerConnection.Configuration.Login(database: "master", authentication: .sqlPassword(username: "u", password: "p"))
        var configuration = SQLServerConnection.Configuration(hostname: "h", login: login, tlsConfiguration: nil, encryptionMode: .optional)
        let optional = try XCTUnwrap(configuration.effectiveTLSConfiguration)
        XCTAssertEqual(optional.certificateVerification, .none)

        configuration.encryptionMode = .mandatory
        XCTAssertNil(configuration.effectiveTLSConfiguration, "Mandatory must not silently skip validation")
    }

    func testSessionResetBatchRestoresDatabaseAndIsolation() {
        let login = SQLServerConnection.Configuration.Login(database: "Sales]DB", authentication: .sqlPassword(username: "u", password: "p"))
        let configuration = SQLServerConnection.Configuration(hostname: "h", login: login)
        let batch = configuration.sessionResetBatch
        XCTAssertTrue(batch.hasPrefix("USE [Sales]]DB];"))
        XCTAssertTrue(batch.contains("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;"))
        XCTAssertTrue(batch.contains("SET XACT_ABORT ON;"))
    }

    func testRoutingTokenIsParsed() throws {
        // USHORT length, protocol 0 (TCP), port 1434, US_VARCHAR "db2"
        let server = Array("db2".utf16)
        var bytes: [UInt8] = [0, 0, 0x00, 0x9A, 0x05, UInt8(server.count), 0]
        for unit in server { bytes.append(UInt8(unit & 0xFF)); bytes.append(UInt8(unit >> 8)) }
        let target = try XCTUnwrap(TDSRequestHandler.parseRouting(bytes))
        XCTAssertEqual(target, TDSRoutingTarget(server: "db2", port: 1434))
        XCTAssertNil(TDSRequestHandler.parseRouting([0, 0, 0x01, 0x9A, 0x05, 0, 0]), "Only TCP routing is valid")
    }

    func testCertificateNameMatchingFollowsRFC6125() {
        XCTAssertTrue(TDSCertificateIdentity.matchesDNS(pattern: "db.example.com", host: "db.example.com"))
        XCTAssertTrue(TDSCertificateIdentity.matchesDNS(pattern: "DB.Example.com.", host: "db.example.com"))
        XCTAssertTrue(TDSCertificateIdentity.matchesDNS(pattern: "*.example.com", host: "db.example.com"))
        XCTAssertFalse(TDSCertificateIdentity.matchesDNS(pattern: "*.example.com", host: "a.db.example.com"), "wildcard covers one label")
        XCTAssertFalse(TDSCertificateIdentity.matchesDNS(pattern: "*.example.com", host: "example.com"))
        XCTAssertFalse(TDSCertificateIdentity.matchesDNS(pattern: "*.com", host: "example.com"), "no wildcard directly under a TLD")
        XCTAssertFalse(TDSCertificateIdentity.matchesDNS(pattern: "d*.example.com", host: "db.example.com"), "partial wildcards are not accepted")
        XCTAssertFalse(TDSCertificateIdentity.matchesDNS(pattern: "other.example.com", host: "db.example.com"))
        XCTAssertEqual(TDSCertificateIdentity.ipBytes("127.0.0.1"), [127, 0, 0, 1])
        XCTAssertEqual(TDSCertificateIdentity.ipBytes("::1")?.count, 16)
        XCTAssertNil(TDSCertificateIdentity.ipBytes("db.example.com"))
    }
}
