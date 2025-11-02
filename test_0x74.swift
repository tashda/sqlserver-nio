import Foundation
import NIO
import Logging

// Simple test to trigger the 0x74 token and see diagnostics
let config = SQLServerClient.Configuration(
    hostname: "192.168.1.200",
    port: 1435,
    login: .init(
        database: "AdventureWorks2022",
        authentication: .sqlPassword(username: "sa", password: "K3nn3th5")
    ),
    tlsConfiguration: .makeClientConfiguration()
)

let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
let client = try await SQLServerClient.connect(configuration: config, eventLoopGroupProvider: .shared(group)).get()

print("🔍 Testing TDS Token 0x74 with HumanResources.vJobCandidate view")

try await client.withConnection { connection in
    print("📋 Querying columns for HumanResources.vJobCandidate...")

    do {
        let columns = try await connection.listColumns(schema: "HumanResources", table: "vJobCandidate").get()
        print("✅ Successfully loaded \(columns.count) columns")

        for (index, column) in columns.prefix(5).enumerated() {
            print("   Column \(index + 1): \(column.name ?? "unnamed") - Type: \(column.typeName ?? "unknown")")
        }

        if columns.count > 5 {
            print("   ... and \(columns.count - 5) more columns")
        }

    } catch {
        print("❌ Error: \(error)")
    }
}

try await client.shutdownGracefully()
try await group.shutdownGracefully()
print("🏁 Test completed")