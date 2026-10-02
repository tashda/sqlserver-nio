import Foundation
import SQLServerKit
import SQLServerKitTesting
import Testing

/// SQL Server Agent categories of each class (msdb.dbo.syscategories.category_class: 1 job,
/// 2 alert, 3 operator). They live in msdb, so Agent itself does not need to be running.
@Suite(.testServer, .serialized) struct AgentCategoryTests {
    @Test func categoriesOfEveryClassAreCreatedListedRenamedAndDeleted() async throws {
        let client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), numberOfThreads: 1)
        defer { Task { try? await client.shutdownGracefully() } }
        let suffix = UUID().uuidString.prefix(8)
        let names = [1: "nio_job_\(suffix)", 2: "nio_alert_\(suffix)", 3: "nio_operator_\(suffix)"]

        for (classId, name) in names { try await client.agent.createCategory(name: name, classId: classId) }
        do {
            let listed = try await client.agent.listCategories()
            for (classId, name) in names {
                #expect(listed.first { $0.name == name }?.classId == classId, "\(name) is listed with class \(classId)")
            }
            // msdb's own categories are listed too, of every class.
            #expect(Set(listed.map(\.classId)) == [1, 2, 3])

            try await client.agent.renameCategory(name: names[2]!, newName: names[2]! + "_renamed", classId: 2)
            let renamed = try await client.agent.listCategories()
            #expect(renamed.contains { $0.name == names[2]! + "_renamed" && $0.classId == 2 })
            try await client.agent.deleteCategory(name: names[2]! + "_renamed", classId: 2)
        } catch {
            for (classId, name) in names { try? await client.agent.deleteCategory(name: name, classId: classId) }
            throw error
        }
        try await client.agent.deleteCategory(name: names[1]!, classId: 1)
        try await client.agent.deleteCategory(name: names[3]!, classId: 3)
        let after = try await client.agent.listCategories()
        #expect(!after.contains { $0.name.hasSuffix(String(suffix)) || $0.name.hasSuffix("\(suffix)_renamed") })
    }

    @Test func anUnknownClassIsRefusedBeforeReachingTheServer() async throws {
        let client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), numberOfThreads: 1)
        defer { Task { try? await client.shutdownGracefully() } }
        await #expect(throws: SQLServerError.self) { try await client.agent.createCategory(name: "nio_bad", classId: 4) }
    }
}
