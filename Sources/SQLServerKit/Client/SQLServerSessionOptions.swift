import Foundation

extension SQLServerConnection {
    public struct SessionOptions: Sendable, Equatable {
        public var ansiNulls: Bool
        public var quotedIdentifier: Bool
        public var ansiPadding: Bool
        public var ansiWarnings: Bool
        public var arithAbort: Bool
        public var concatNullYieldsNull: Bool
        public var implicitTransactions: Bool
        public var nocount: Bool
        public var fmtOnlyOff: Bool
        public var language: String?
        public var dateFormat: String?
        public var additionalStatements: [String]

        public init(
            ansiNulls: Bool = true,
            quotedIdentifier: Bool = true,
            ansiPadding: Bool = true,
            ansiWarnings: Bool = true,
            arithAbort: Bool = true,
            concatNullYieldsNull: Bool = true,
            implicitTransactions: Bool = false,
            nocount: Bool = true,
            fmtOnlyOff: Bool = true,
            language: String? = nil,
            dateFormat: String? = nil,
            additionalStatements: [String] = []
        ) {
            self.ansiNulls = ansiNulls
            self.quotedIdentifier = quotedIdentifier
            self.ansiPadding = ansiPadding
            self.ansiWarnings = ansiWarnings
            self.arithAbort = arithAbort
            self.concatNullYieldsNull = concatNullYieldsNull
            self.implicitTransactions = implicitTransactions
            self.nocount = nocount
            self.fmtOnlyOff = fmtOnlyOff
            self.language = language
            self.dateFormat = dateFormat
            self.additionalStatements = additionalStatements
        }

        public static var ssmsDefaults: SessionOptions {
            SessionOptions(
                ansiNulls: true,
                quotedIdentifier: true,
                ansiPadding: true,
                ansiWarnings: true,
                arithAbort: true,
                concatNullYieldsNull: true,
                implicitTransactions: false,
                nocount: true,
                fmtOnlyOff: true,
                language: nil,
                dateFormat: nil,
                additionalStatements: []
            )
        }

        internal func buildStatements() -> [String] {
            var statements: [String] = []
            func append(_ keyword: String, _ enabled: Bool) {
                let value = enabled ? "ON" : "OFF"
                statements.append("SET \(keyword) \(value);")
            }
            append("ANSI_NULLS", ansiNulls)
            append("QUOTED_IDENTIFIER", quotedIdentifier)
            append("ANSI_PADDING", ansiPadding)
            append("ANSI_WARNINGS", ansiWarnings)
            append("ARITHABORT", arithAbort)
            append("CONCAT_NULL_YIELDS_NULL", concatNullYieldsNull)
            append("IMPLICIT_TRANSACTIONS", implicitTransactions)
            append("NOCOUNT", nocount)
            if fmtOnlyOff {
                statements.append("SET FMTONLY OFF;")
            }
            if let language {
                statements.append("SET LANGUAGE N'\(Self.escapeLiteral(language))';")
            }
            if let dateFormat {
                statements.append("SET DATEFORMAT \(dateFormat);")
            }
            if !additionalStatements.isEmpty {
                statements.append(contentsOf: additionalStatements)
            }
            return statements
        }

        private static func escapeLiteral(_ value: String) -> String {
            value.replacingOccurrences(of: "'", with: "''")
        }
    }
}
