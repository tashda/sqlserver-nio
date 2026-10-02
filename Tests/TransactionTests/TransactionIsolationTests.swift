import SQLServerKit
import SQLServerKitTesting
import XCTest
import NIO
import Dispatch

final class TransactionIsolationTests: TransactionTestBase, @unchecked Sendable {
    func testTransactionIsolation() async throws {
        let tableName = "tx_iso_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        let adminClient = SQLServerAdministrationClient(client: self.client)

        try await adminClient.createTable(name: tableName, columns: [
            .init(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            .init(name: "value", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
        ])
        try await self.client.withConnection { connection in
            try await connection.insertRow(into: tableName, values: ["id": .int(1), "value": .nString("Original")])
        }

        let connection1Result = try await self.client.withConnection { connection1 in
            try await connection1.beginTransaction()
            try await connection1.updateRows(in: tableName, set: ["value": .nString("Modified")], where: "id = 1")
            let rows = try await connection1.query("SELECT value FROM [\(tableName)] WHERE id = 1")
            let value = rows.first?.column("value")?.string
            try await connection1.rollback()
            return value
        }

        let connection2Result = try await self.client.withConnection { connection2 in
            let rows = try await connection2.query("SELECT value FROM [\(tableName)] WHERE id = 1")
            return rows.first?.column("value")?.string
        }

        XCTAssertEqual(connection1Result, "Modified", "Should see modified value within transaction")
        XCTAssertEqual(connection2Result, "Original", "Should see original value after rollback")
    }

    func testReadCommittedPreventsDirtyReads() async throws {
        let tableName = "tx_rc_\(UUID().uuidString.prefix(8))"
        let adminClient = SQLServerAdministrationClient(client: self.client)
        try await adminClient.createTable(name: tableName, columns: [
            .init(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            .init(name: "value", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
        ])
        try await self.client.withConnection { connection in
            try await connection.insertRow(into: tableName, values: ["id": .int(1), "value": .nString("Original")])
        }

        let writer = Task {
            try await self.client.withConnection { connection in
                try await connection.beginTransaction()
                try await connection.updateRows(in: tableName, set: ["value": .nString("Dirty")], where: "id = 1")
                try await Task.sleep(nanoseconds: 500_000_000)
                try await connection.rollback()
            }
        }

        try await Task.sleep(nanoseconds: 100_000_000)

        let readValue = try await self.client.withConnection { connection in
            try await connection.setIsolationLevel(.readCommitted)
            let rows = try await connection.query("SELECT value FROM [\(tableName)] WHERE id = 1")
            return rows.first?.column("value")?.string
        }
        XCTAssertEqual(readValue, "Original", "READ COMMITTED should block until dirty update rolls back")

        _ = try await withTimeout(10) { try await writer.value }
    }

    func testReadUncommittedAllowsDirtyReads() async throws {
        let tableName = "tx_ru_\(UUID().uuidString.prefix(8))"
        let adminClient = SQLServerAdministrationClient(client: self.client)
        try await adminClient.createTable(name: tableName, columns: [
            .init(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            .init(name: "value", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
        ])
        try await self.client.withConnection { connection in
            try await connection.insertRow(into: tableName, values: ["id": .int(1), "value": .nString("Original")])
        }

        let writer = Task {
            try await self.client.withConnection { connection in
                try await connection.beginTransaction()
                try await connection.updateRows(in: tableName, set: ["value": .nString("Dirty")], where: "id = 1")
                try await Task.sleep(nanoseconds: 500_000_000)
                try await connection.rollback()
            }
        }

        try await Task.sleep(nanoseconds: 100_000_000)

        let dirtyRead = try await client.withConnection { connection in
            try await connection.setIsolationLevel(.readUncommitted)
            let rows = try await connection.query("SELECT value FROM [\(tableName)] WHERE id = 1")
            return rows.first?.column("value")?.string
        }
        XCTAssertEqual(dirtyRead, "Dirty", "READ UNCOMMITTED should see uncommitted changes")

        _ = try await withTimeout(10) { try await writer.value }
    }

    func testRepeatableReadPreventsNonRepeatableReads() async throws {
        let tableName = "tx_rr_\(UUID().uuidString.prefix(8))"
        let adminClient = SQLServerAdministrationClient(client: self.client)
        try await adminClient.createTable(name: tableName, columns: [
            .init(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            .init(name: "value", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
        ])
        try await client.withConnection { connection in
            try await connection.insertRow(into: tableName, values: ["id": .int(1), "value": .nString("Original")])
        }

        let blocker = Task {
            try await self.client.withConnection { connection in
                try await connection.setIsolationLevel(.repeatableRead)
                try await connection.beginTransaction()
                let first = try await connection.query("SELECT value FROM [\(tableName)] WHERE id = 1")
                try await Task.sleep(nanoseconds: 600_000_000)
                let second = try await connection.query("SELECT value FROM [\(tableName)] WHERE id = 1")
                try await connection.commit()
                return (first.first?.column("value")?.string, second.first?.column("value")?.string)
            }
        }

        try await Task.sleep(nanoseconds: 100_000_000)

        _ = try await client.withConnection { connection in
            try await connection.updateRows(in: tableName, set: ["value": .nString("Updated")], where: "id = 1")
        }

        let (firstRead, secondRead) = try await withTimeout(10) { try await blocker.value }
        XCTAssertEqual(firstRead, "Original")
        XCTAssertEqual(secondRead, "Original")

        let finalValue = try await client.withConnection { connection in
            try await connection.query("SELECT value FROM [\(tableName)] WHERE id = 1")
        }
        XCTAssertEqual(finalValue.first?.column("value")?.string, "Updated")
    }

    func testSerializablePreventsPhantomInserts() async throws {
        let tableName = "tx_ser_\(UUID().uuidString.prefix(8))"
        let adminClient = SQLServerAdministrationClient(client: self.client)
        try await adminClient.createTable(name: tableName, columns: [
            .init(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            .init(name: "category", definition: .standard(.init(dataType: .nvarchar(length: .length(10)))))
        ])
        try await client.withConnection { connection in
            try await connection.insertRow(into: tableName, values: ["id": .int(1), "category": .nString("A")])
            try await connection.insertRow(into: tableName, values: ["id": .int(2), "category": .nString("A")])
        }

        let rangeLockTask = Task {
            try await self.client.withConnection { connection in
                try await connection.setIsolationLevel(.serializable)
                try await connection.beginTransaction()
                _ = try await connection.query("SELECT COUNT(*) FROM [\(tableName)] WHERE category = N'A'")
                try await Task.sleep(nanoseconds: 700_000_000)
                try await connection.commit()
            }
        }

        try await Task.sleep(nanoseconds: 200_000_000)

        let insertDelay = try await self.client.withConnection { connection in
            let start = DispatchTime.now()
            try await connection.insertRow(into: tableName, values: ["id": .int(3), "category": .nString("A")])
            let elapsed = DispatchTime.now().uptimeNanoseconds - start.uptimeNanoseconds
            return elapsed
        }

        _ = try await withTimeout(10) { try await rangeLockTask.value }
        XCTAssertGreaterThan(insertDelay, 50_000_000 as UInt64, "INSERT should have experienced some delay due to SERIALIZABLE transaction")

        let countResult = try await self.client.withConnection { connection in
            try await connection.query("SELECT COUNT(*) as count FROM [\(tableName)] WHERE category = N'A'")
        }
        XCTAssertEqual(countResult.first?.column("count")?.int, 3)
    }

    func testSnapshotIsolationProvidesStableView() async throws {
        try await ensureSnapshotIsolationEnabled()
        let tableName = "tx_snap_\(UUID().uuidString.prefix(8))"
        let adminClient = SQLServerAdministrationClient(client: self.client)
        try await adminClient.createTable(name: tableName, columns: [
            .init(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            .init(name: "value", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
        ])
        try await self.client.withConnection { connection in
            try await connection.insertRow(into: tableName, values: ["id": .int(1), "value": .nString("Original")])
        }

        let writer = Task {
            try await self.client.withConnection { connection in
                try await connection.beginTransaction()
                try await connection.updateRows(in: tableName, set: ["value": .nString("NewValue")], where: "id = 1")
                try await Task.sleep(nanoseconds: 500_000_000)
                try await connection.commit()
            }
        }

        try await Task.sleep(nanoseconds: 100_000_000)

        let (firstRead, secondRead) = try await self.client.withConnection { connection in
            try await connection.setIsolationLevel(.snapshot)
            try await connection.beginTransaction()
            let first = try await connection.query("SELECT value FROM [\(tableName)] WHERE id = 1")
            try await Task.sleep(nanoseconds: 400_000_000)
            let second = try await connection.query("SELECT value FROM [\(tableName)] WHERE id = 1")
            try await connection.commit()
            return (first.first?.column("value")?.string, second.first?.column("value")?.string)
        }

        _ = try await withTimeout(10) { try await writer.value }

        XCTAssertEqual(firstRead, "Original")
        XCTAssertEqual(secondRead, "Original")

        let committedValue = try await self.client.withConnection { connection in
            try await connection.query("SELECT value FROM [\(tableName)] WHERE id = 1")
        }
        XCTAssertEqual(committedValue.first?.column("value")?.string, "NewValue")
    }
}
