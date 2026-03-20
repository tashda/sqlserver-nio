import XCTest
import NIOCore
@testable import SQLServerKit
@testable import SQLServerTDS

final class SQLServerClrUdtDisplayTests: XCTestCase, @unchecked Sendable {
    func testHierarchyIdDescriptionRendersCanonicalPath() {
        var buffer = ByteBufferAllocator().buffer(capacity: 3)
        buffer.writeBytes([0x7C, 0x2B, 0x78])

        let value = SQLServerValue(base: TDSData(metadata: hierarchyIDMetadata, value: buffer))

        XCTAssertEqual(value.udtTypeName, "hierarchyid")
        XCTAssertEqual(value.description, "/3/4/1/3/")
    }

    func testHierarchyIdDescriptionRendersAdventureWorksPath() {
        var buffer = ByteBufferAllocator().buffer(capacity: 2)
        buffer.writeBytes([0x5A, 0xDE])

        let value = SQLServerValue(base: TDSData(metadata: hierarchyIDMetadata, value: buffer))

        XCTAssertEqual(value.description, "/1/1/3/")
    }

    func testHierarchyIdDescriptionRendersDottedNegativePath() {
        var buffer = ByteBufferAllocator().buffer(capacity: 3)
        buffer.writeBytes([0x54, 0x07, 0x30])

        let value = SQLServerValue(base: TDSData(metadata: hierarchyIDMetadata, value: buffer))

        XCTAssertEqual(value.description, "/0.3.-7/")
    }

    func testHierarchyIdDescriptionRendersRootForEmptyPayload() {
        let value = SQLServerValue(
            base: TDSData(
                metadata: hierarchyIDMetadata,
                value: ByteBufferAllocator().buffer(capacity: 0)
            )
        )

        XCTAssertEqual(value.description, "/")
    }

    func testNonHierarchyClrUdtStillRendersHex() {
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
            colName: "SomeUdt",
            udtInfo: .init(
                databaseName: "sys",
                schemaName: "dbo",
                typeName: "geography",
                assemblyName: "Microsoft.SqlServer.Types"
            )
        )

        let value = SQLServerValue(base: TDSData(metadata: metadata, value: buffer))

        XCTAssertEqual(value.description, "0x7C2B78")
    }
}

private let hierarchyIDMetadata = TDSTokens.ColMetadataToken.ColumnData(
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
