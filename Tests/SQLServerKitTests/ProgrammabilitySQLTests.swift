import Foundation
@testable import SQLServerKit
import Testing

@Suite struct ProgrammabilitySQLTests {
    @Test func inlineTableValuedFunction() {
        let sql = SQLServerRoutineClient.inlineTableValuedFunctionSQL(
            name: "OrdersFor",
            parameters: [FunctionParameter(name: "customer", dataType: .int)],
            query: "SELECT 1 AS Id",
            schema: "sales",
            options: RoutineOptions()
        )
        #expect(sql == "CREATE FUNCTION [sales].[OrdersFor]\n(\n    @customer INT\n)\nRETURNS TABLE\nAS\nRETURN\n(\nSELECT 1 AS Id\n)")
    }

    @Test func aliasType() {
        #expect(SQLServerTypeClient.aliasTypeSQL(name: "Phone", schema: "dbo", baseType: .varchar(length: .length(20)), isNullable: false)
            == "CREATE TYPE [dbo].[Phone] FROM VARCHAR(20) NOT NULL")
    }

    @Test func sequenceWithEveryOption() {
        let sql = SQLServerAdministrationClient.sequenceSQL(
            name: "OrderNumbers", schema: "dbo", type: .int, start: 1000, increment: 5,
            minValue: 1000, maxValue: 9999, cycle: true, cache: .size(50)
        )
        #expect(sql == "CREATE SEQUENCE [dbo].[OrderNumbers] AS INT START WITH 1000 INCREMENT BY 5 MINVALUE 1000 MAXVALUE 9999 CYCLE CACHE 50")
    }

    @Test func sequenceDefaults() {
        let sql = SQLServerAdministrationClient.sequenceSQL(
            name: "S", schema: "dbo", type: .bigint, start: nil, increment: 1,
            minValue: nil, maxValue: nil, cycle: false, cache: .none
        )
        #expect(sql == "CREATE SEQUENCE [dbo].[S] AS BIGINT INCREMENT BY 1 NO MINVALUE NO MAXVALUE NO CYCLE NO CACHE")
    }

    @Test func synonymTargetNames() {
        #expect(SQLServerObjectName(object: "Orders").sql == "[dbo].[Orders]")
        #expect(SQLServerObjectName(server: "Remote", database: "Sales", schema: "s", object: "T").sql == "[Remote].[Sales].[s].[T]")
    }
}

@Suite struct CertificateSQLTests {
    @Test func masterKeyAndCertificate() {
        #expect(SQLServerSecurityClient.masterKeySQL(password: "p'w") == "CREATE MASTER KEY ENCRYPTION BY PASSWORD = N'p''w'")
        #expect(SQLServerSecurityClient.certificateSQL(name: "LabCert", subject: "Lab", expiryDate: Date(timeIntervalSince1970: 1_893_456_000))
            == "CREATE CERTIFICATE [LabCert] WITH SUBJECT = N'Lab', EXPIRY_DATE = '20300101'")
    }
}
