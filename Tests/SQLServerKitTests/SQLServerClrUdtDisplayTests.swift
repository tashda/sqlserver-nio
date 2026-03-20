import XCTest
import NIOCore
@testable import SQLServerKit
@testable import SQLServerTDS

final class SQLServerClrUdtDisplayTests: XCTestCase, @unchecked Sendable {
    func testHierarchyIdDescriptionRendersHexValue() {
        var buffer = ByteBufferAllocator().buffer(capacity: 3)
        buffer.writeBytes([0x7C, 0x2B, 0x78])

        let metadata = TDSTokens.ColMetadataToken.ColumnData(
            userType: 0,
            flags: 0,
            dataType: .clrUdt,
            length: 128,
            precision: 0,
            scale: 0,
            collation: [],
            colName: "OrganizationNode",
            udtInfo: .init(
                databaseName: "sys",
                schemaName: "dbo",
                typeName: "hierarchyid",
                assemblyName: "Microsoft.SqlServer.Types"
            )
        )

        let value = SQLServerValue(base: TDSData(metadata: metadata, value: buffer))

        XCTAssertEqual(value.udtTypeName, "hierarchyid")
        XCTAssertEqual(value.description, "0x7C2B78")
    }
}
