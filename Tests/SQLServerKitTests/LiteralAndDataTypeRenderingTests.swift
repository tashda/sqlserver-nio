import Foundation
@testable import SQLServerKit
import Testing

@Suite struct LiteralAndDataTypeRenderingTests {
    @Test(arguments: [
        (SQLDataType.rowversion, "ROWVERSION"),
        (.hierarchyid, "HIERARCHYID"),
        (.geometry, "GEOMETRY"),
        (.geography, "GEOGRAPHY"),
        (.json, "JSON"),
        (.vector(dimensions: 3), "VECTOR(3)"),
    ])
    func newDataTypesRender(type: SQLDataType, expected: String) {
        #expect(type.sqlLiteral == expected)
    }

    @Test func variantKeepsTheBaseTypeOfEachValue() {
        #expect(SQLServerLiteralValue.variant(.int(42)).sqlLiteral() == "CAST(42 AS SQL_VARIANT)")
        #expect(SQLServerLiteralValue.variant(.nString("it's")).sqlLiteral() == "CAST(N'it''s' AS SQL_VARIANT)")
    }

    @Test func spatialAndHierarchyValuesRender() {
        #expect(SQLServerLiteralValue.geometry(wellKnownText: "POINT (1 2)", srid: 0).sqlLiteral()
            == "geometry::STGeomFromText(N'POINT (1 2)', 0)")
        #expect(SQLServerLiteralValue.geography(wellKnownText: "POINT (12.57 55.68)", srid: 4326).sqlLiteral()
            == "geography::STGeomFromText(N'POINT (12.57 55.68)', 4326)")
        #expect(SQLServerLiteralValue.hierarchyID("/1/3/").sqlLiteral() == "hierarchyid::Parse(N'/1/3/')")
    }
}
