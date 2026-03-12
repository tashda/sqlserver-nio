import Foundation
import NIO
import SQLServerTDS

extension SQLServerMetadataOperations {
    // MARK: - Procedures & Functions

    public func objectDefinition(
        database: String? = nil,
        schema: String,
        name: String,
        kind: SQLServerMetadataObjectIdentifier.Kind
    ) -> EventLoopFuture<ObjectDefinition?> {
        let identifier = SQLServerMetadataObjectIdentifier(database: database, schema: schema, name: name, kind: kind)
        return fetchObjectDefinitions([identifier]).map(\.first)
    }

    public func listProcedures(
        database: String? = nil,
        schema: String? = nil,
        includeComments: Bool = false
    ) -> EventLoopFuture<[RoutineMetadata]> {
        let resolvedDatabase = effectiveDatabase(database)
        let dbPrefix = resolvedDatabase.map { "[\(SQLServerMetadataOperations.escapeIdentifier($0))]." } ?? ""

        var predicates: [String] = ["1=1"]
        if let schema {
            predicates.append("s.name = N'\(SQLServerMetadataOperations.escapeLiteral(schema))'")
        } else if !self.configuration.includeSystemSchemas {
            predicates.append("s.name NOT IN ('sys', 'INFORMATION_SCHEMA')")
        }
        predicates.append("p.name NOT LIKE 'meta_client_%'")
        let whereClause = predicates.joined(separator: " AND ")

        let definitionSelect = self.configuration.includeRoutineDefinitions ? ", CAST(m.definition AS NVARCHAR(4000)) AS definition" : ""
        let commentSelect = includeComments ? ", ISNULL(CAST(ep.value AS NVARCHAR(4000)), '') AS comment" : ""
        let joinModules = self.configuration.includeRoutineDefinitions ? "LEFT JOIN \(dbPrefix)sys.sql_modules AS m WITH (NOLOCK) ON m.object_id = p.object_id AND p.modify_date >= DATEADD(MINUTE, -5, SYSDATETIME())" : ""
        let joinComments = includeComments ? "LEFT JOIN \(dbPrefix)sys.extended_properties AS ep WITH (NOLOCK) ON ep.class = 1 AND ep.major_id = p.object_id AND ep.minor_id = 0 AND ep.name = N'MS_Description'" : ""

        let sql = """
        SELECT
            schema_name = s.name,
            object_name = p.name\(definitionSelect)\(commentSelect)
        FROM \(dbPrefix)sys.procedures AS p WITH (NOLOCK)
        JOIN \(dbPrefix)sys.schemas AS s WITH (NOLOCK) ON s.schema_id = p.schema_id
        \(joinModules)
        \(joinComments)
        WHERE \(whereClause)
        ORDER BY s.name, p.name;
        """

        return queryExecutor(sql).map { rows in
            rows.compactMap { row in
                guard let schemaName = row.column("schema_name")?.string, let name = row.column("object_name")?.string else { return nil }
                let def = self.configuration.includeRoutineDefinitions ? row.column("definition")?.string : nil
                return RoutineMetadata(schema: schemaName, name: name, type: .procedure, definition: def, isSystemObject: false, comment: row.column("comment")?.string)
            }
        }
    }

    public func listFunctions(
        database: String? = nil,
        schema: String? = nil,
        includeComments: Bool = false
    ) -> EventLoopFuture<[RoutineMetadata]> {
        let resolvedDatabase = effectiveDatabase(database)
        let dbPrefix = resolvedDatabase.map { "[\(SQLServerMetadataOperations.escapeIdentifier($0))]." } ?? ""

        var predicates: [String] = ["o.type IN ('FN', 'TF', 'IF')"]
        if let schema {
            predicates.append("s.name = N'\(SQLServerMetadataOperations.escapeLiteral(schema))'")
        } else if !self.configuration.includeSystemSchemas {
            predicates.append("s.name NOT IN ('sys', 'INFORMATION_SCHEMA')")
        }
        predicates.append("o.name NOT LIKE 'meta_client_%'")
        let whereClause = predicates.joined(separator: " AND ")

        let definitionSelect = self.configuration.includeRoutineDefinitions ? ", CAST(m.definition AS NVARCHAR(4000)) AS definition" : ""
        let commentSelect = includeComments ? ", ISNULL(CAST(ep.value AS NVARCHAR(4000)), '') AS comment" : ""
        let joinModules = self.configuration.includeRoutineDefinitions ? "LEFT JOIN \(dbPrefix)sys.sql_modules AS m WITH (NOLOCK) ON m.object_id = o.object_id AND o.modify_date >= DATEADD(MINUTE, -5, SYSDATETIME())" : ""
        let joinComments = includeComments ? "LEFT JOIN \(dbPrefix)sys.extended_properties AS ep WITH (NOLOCK) ON ep.class = 1 AND ep.major_id = o.object_id AND ep.minor_id = 0 AND ep.name = N'MS_Description'" : ""

        let sql = """
        SELECT
            schema_name = s.name,
            object_name = o.name,
            type_desc = o.type_desc,
            is_ms_shipped = o.is_ms_shipped\(definitionSelect)\(commentSelect)
        FROM \(dbPrefix)sys.objects AS o WITH (NOLOCK)
        JOIN \(dbPrefix)sys.schemas AS s WITH (NOLOCK) ON s.schema_id = o.schema_id
        \(joinModules)
        \(joinComments)
        WHERE \(whereClause)
        ORDER BY s.name, o.name;
        """

        return queryExecutor(sql).map { rows in
            rows.compactMap { row -> RoutineMetadata? in
                guard
                    let schemaName = row.column("schema_name")?.string,
                    let objectName = row.column("object_name")?.string,
                    let typeDesc = row.column("type_desc")?.string
                else {
                    return nil
                }
                let objectType = ObjectDefinition.ObjectType.from(typeDesc: typeDesc)
                let def = self.configuration.includeRoutineDefinitions ? row.column("definition")?.string : nil
                return RoutineMetadata(
                    schema: schemaName,
                    name: objectName,
                    type: objectType == .tableFunction ? .tableFunction : .scalarFunction,
                    definition: def,
                    isSystemObject: row.column("is_ms_shipped")?.bool ?? false,
                    comment: row.column("comment")?.string
                )
            }
        }
    }

    public func listParameters(
        database: String? = nil,
        schema: String,
        object: String
    ) -> EventLoopFuture<[ParameterMetadata]> {
        let resolvedDatabase = effectiveDatabase(database)
        let dbPrefix = resolvedDatabase.map { "[\(SQLServerMetadataOperations.escapeIdentifier($0))]." } ?? ""
        let escapedSchema = SQLServerMetadataOperations.escapeLiteral(schema)
        let escapedObject = SQLServerMetadataOperations.escapeLiteral(object)

        let sql = """
        SELECT schema_name = s.name, object_name = o.name, object_type = o.type, parameter_id = p.parameter_id,
               parameter_name = p.name, user_type_name = ut.name, system_type_name = st.name,
               max_length = p.max_length, precision = p.precision, scale = p.scale,
               is_output = p.is_output, is_readonly = ISNULL(p.is_readonly, 0), has_default_value = p.has_default_value
        FROM \(dbPrefix)sys.objects AS o JOIN \(dbPrefix)sys.schemas AS s ON s.schema_id = o.schema_id
        LEFT JOIN \(dbPrefix)sys.parameters AS p ON p.object_id = o.object_id
        LEFT JOIN \(dbPrefix)sys.types AS ut ON ut.user_type_id = p.user_type_id AND ut.system_type_id = ut.user_type_id
        LEFT JOIN \(dbPrefix)sys.types AS st ON st.system_type_id = p.system_type_id AND st.user_type_id = st.system_type_id 
        WHERE s.name = N'\(escapedSchema)' AND o.name = N'\(escapedObject)' AND o.type IN ('P','PC','RF','AF','FN','TF','IF')
        ORDER BY p.parameter_id;
        """

        return queryExecutor(sql).flatMap { rows in
            let textDefaultsFuture = rows.isEmpty ? self.eventLoop.makeSucceededFuture([String: (hasDefault: Bool, defaultValue: String?)]()) : self.loadParameterDefaults(database: database, schema: schema, object: object)
            return textDefaultsFuture.flatMap { defaults in
                let objectType = rows.first?.column("object_type")?.string?.uppercased() ?? ""
                let isFunctionObject = ["FN","TF","IF","AF","RF"].contains(objectType)
                let mapped = rows.compactMap { row -> ParameterMetadata? in
                    guard let sName = row.column("schema_name")?.string, let rawObjName = row.column("object_name")?.string,
                          let name = row.column("parameter_name")?.string, let ordinal = row.column("parameter_id")?.int else { return nil }
                    let typeName = row.column("user_type_name")?.string ?? row.column("system_type_name")?.string
                    let objName = SQLServerMetadataOperations.normalizeRoutineName(rawObjName)
                    let normName = name.lowercased()
                    let override = defaults[normName]
                    let hasDefault = (override?.hasDefault ?? (row.column("has_default_value")?.bool ?? false))
                    return ParameterMetadata(schema: sName, object: objName, name: name, ordinal: ordinal, isReturnValue: (ordinal == 0 || normName == "@return_value"),
                                             typeName: typeName ?? "", systemTypeName: row.column("system_type_name")?.string,
                                             maxLength: row.column("max_length")?.int, precision: row.column("precision")?.int, scale: row.column("scale")?.int,
                                             isOutput: row.column("is_output")?.bool ?? false, hasDefaultValue: hasDefault, defaultValue: override?.defaultValue, isReadOnly: row.column("is_readonly")?.bool ?? false)
                }.filter { isFunctionObject ? true : !$0.isReturnValue }
                return self.eventLoop.makeSucceededFuture(mapped)
            }
        }
    }

    internal func fetchObjectDefinitions(_ identifiers: [SQLServerMetadataObjectIdentifier]) -> EventLoopFuture<[ObjectDefinition]> {
        let initial = eventLoop.makeSucceededFuture([ObjectDefinition]())
        return identifiers.reduce(initial) { partial, target in
            partial.flatMap { collected in
                let dbPrefix = target.database.map { "[\(SQLServerMetadataOperations.escapeIdentifier($0))]." } ?? ""
                let sql = "SELECT s.name as schema_name, o.name as object_name, o.type_desc, o.is_ms_shipped, o.create_date, o.modify_date FROM \(dbPrefix)sys.objects o JOIN \(dbPrefix)sys.schemas s ON o.schema_id = s.schema_id WHERE s.name = N'\(SQLServerMetadataOperations.escapeLiteral(target.schema))' AND o.name = N'\(SQLServerMetadataOperations.escapeLiteral(target.name))'"
                return self.queryExecutor(sql).flatMap { rows in
                    guard let row = rows.first, let s = row.column("schema_name")?.string, let o = row.column("object_name")?.string, let td = row.column("type_desc")?.string else { return self.eventLoop.makeSucceededFuture(collected) }
                    let type = ObjectDefinition.ObjectType.from(typeDesc: td)
                    
                    let defF: EventLoopFuture<String?>
                    if type == .table {
                        defF = self.scriptTableDefinition(database: target.database, schema: s, table: o)
                    } else {
                        defF = self.fetchModuleDefinitionWithPreamble(database: target.database, schema: s, object: o, type: type, dbPrefix: dbPrefix)
                    }
                    
                    return defF.map { def in collected + [ObjectDefinition(schema: s, name: o, type: type, definition: def, isSystemObject: row.column("is_ms_shipped")?.bool ?? false, createDate: row.column("create_date")?.date, modifyDate: row.column("modify_date")?.date)] }
                }
            }
        }
    }

    internal func loadParameterDefaults(database: String?, schema: String, object: String) -> EventLoopFuture<[String: (hasDefault: Bool, defaultValue: String?)]> {
        guard configuration.extractParameterDefaults else { return eventLoop.makeSucceededFuture([:]) }
        let candidates: [SQLServerMetadataObjectIdentifier] = [
            .init(database: database, schema: schema, name: object, kind: .procedure),
            .init(database: database, schema: schema, name: object, kind: .function)
        ]
        return fetchObjectDefinitions(candidates).map { defs in
            guard let def = defs.first(where: { $0.definition?.isEmpty == false })?.definition else { return [:] }
            return SQLServerMetadataOperations.extractParameterDefaults(from: def)
        }
    }

    internal func fetchModuleDefinitionWithPreamble(database: String?, schema: String, object: String, type: ObjectDefinition.ObjectType, dbPrefix: String) -> EventLoopFuture<String?> {
        let defSql = """
        SELECT CAST(m.definition AS NVARCHAR(4000)) AS definition
        FROM \(dbPrefix)sys.objects AS o
        JOIN \(dbPrefix)sys.schemas AS s ON s.schema_id = o.schema_id
        JOIN \(dbPrefix)sys.sql_modules AS m ON m.object_id = o.object_id
        WHERE s.name = N'\(SQLServerMetadataOperations.escapeLiteral(schema))' AND o.name = N'\(SQLServerMetadataOperations.escapeLiteral(object))';
        """
        let preambleSql = """
        SELECT m.uses_ansi_nulls, m.uses_quoted_identifier
        FROM \(dbPrefix)sys.objects AS o
        JOIN \(dbPrefix)sys.schemas AS s ON s.schema_id = o.schema_id
        LEFT JOIN \(dbPrefix)sys.sql_modules AS m ON m.object_id = o.object_id
        WHERE s.name = N'\(SQLServerMetadataOperations.escapeLiteral(schema))' AND o.name = N'\(SQLServerMetadataOperations.escapeLiteral(object))';
        """

        let defFuture = queryExecutor(defSql).map { rows -> String? in
            guard let text = rows.first?.column("definition")?.string, !text.isEmpty else { return nil }
            return text.trimmingCharacters(in: .whitespacesAndNewlines)
        }
        let preambleFuture = queryExecutor(preambleSql).map { rows -> String in
            guard let row = rows.first else { return "" }
            let ansi = (row.column("uses_ansi_nulls")?.int ?? 1) != 0
            let qi = (row.column("uses_quoted_identifier")?.int ?? 1) != 0
            return "SET ANSI_NULLS \(ansi ? "ON" : "OFF")\nGO\nSET QUOTED_IDENTIFIER \(qi ? "ON" : "OFF")\nGO\n"
        }

        return defFuture.and(preambleFuture).flatMap { body, preamble in
            guard let body else {
                return self.eventLoop.makeSucceededFuture(nil)
            }

            if type == .view {
                return self.fetchObjectIndexDetails(database: database, schema: schema, object: object).map { indexes in
                    var script = preamble + body
                    let constraintNames = Set<String>()
                    for index in indexes where !constraintNames.contains(index.name) {
                        if index.indexType == 5 || index.indexType == 6 {
                            let kind = index.indexType == 5 ? "CLUSTERED COLUMNSTORE" : "NONCLUSTERED COLUMNSTORE"
                            var statement = "\n\nCREATE \(kind) INDEX [\(SQLServerMetadataOperations.escapeIdentifier(index.name))] ON [\(SQLServerMetadataOperations.escapeIdentifier(schema))].[\(SQLServerMetadataOperations.escapeIdentifier(object))]"
                            if index.indexType == 6 {
                                let columns = index.columns
                                    .sorted { $0.ordinal < $1.ordinal }
                                    .map { "[\(SQLServerMetadataOperations.escapeIdentifier($0.column))]" }
                                    .joined(separator: ", ")
                                if !columns.isEmpty {
                                    statement += " (\(columns))"
                                }
                            }
                            if let options = index.optionClause, !options.isEmpty {
                                statement += " WITH (\(options))"
                            }
                            if let storage = index.storageClause, !storage.isEmpty {
                                statement += " \(storage)"
                            }
                            statement += ";"
                            script += statement
                            continue
                        }

                        let keyColumns = index.columns
                            .filter { !$0.isIncluded }
                            .sorted { $0.ordinal < $1.ordinal }
                            .map { "[\(SQLServerMetadataOperations.escapeIdentifier($0.column))] \($0.isDescending ? "DESC" : "ASC")" }
                            .joined(separator: ", ")
                        var statement = "\n\nCREATE \(index.isUnique ? "UNIQUE " : "")\(index.isClustered ? "CLUSTERED" : "NONCLUSTERED") INDEX [\(SQLServerMetadataOperations.escapeIdentifier(index.name))] ON [\(SQLServerMetadataOperations.escapeIdentifier(schema))].[\(SQLServerMetadataOperations.escapeIdentifier(object))] (\(keyColumns))"
                        let includedColumns = index.columns
                            .filter { $0.isIncluded }
                            .map { "[\(SQLServerMetadataOperations.escapeIdentifier($0.column))]" }
                        if !includedColumns.isEmpty {
                            statement += " INCLUDE (\(includedColumns.joined(separator: ", ")))"
                        }
                        if let filter = index.filterDefinition, !filter.isEmpty {
                            statement += " WHERE \(filter)"
                        }
                        if let options = index.optionClause, !options.isEmpty {
                            statement += " WITH (\(options))"
                        }
                        if let storage = index.storageClause, !storage.isEmpty {
                            statement += " \(storage)"
                        }
                        statement += ";"
                        script += statement
                    }
                    return script
                }
            }

            return self.eventLoop.makeSucceededFuture(preamble + body)
        }
    }

    internal static func extractParameterDefaults(from definition: String) -> [String: (hasDefault: Bool, defaultValue: String?)] {
        let text = definition
        let lower = text.lowercased()

        func convertIndex(_ idx: String.Index) -> String.Index {
            let distance = lower.distance(from: lower.startIndex, to: idx)
            return text.index(text.startIndex, offsetBy: distance)
        }

        func splitTopLevel(byComma span: Substring) -> [String] {
            var parts: [String] = []
            var current = ""
            var depth = 0
            var inSingle = false
            var inBracket = false
            var prev: Character = "\u{0}"

            for ch in span {
                if inSingle {
                    current.append(ch)
                    if ch == "'" && prev != "'" {
                        inSingle = false
                    }
                } else if inBracket {
                    current.append(ch)
                    if ch == "]" {
                        inBracket = false
                    }
                } else {
                    switch ch {
                    case "'":
                        inSingle = true
                        current.append(ch)
                    case "[":
                        inBracket = true
                        current.append(ch)
                    case "(":
                        depth += 1
                        current.append(ch)
                    case ")":
                        depth = max(0, depth - 1)
                        current.append(ch)
                    case ",":
                        if depth == 0 {
                            parts.append(current.trimmingCharacters(in: .whitespacesAndNewlines))
                            current.removeAll(keepingCapacity: true)
                        } else {
                            current.append(ch)
                        }
                    default:
                        current.append(ch)
                    }
                }
                prev = ch
            }

            let tail = current.trimmingCharacters(in: .whitespacesAndNewlines)
            if !tail.isEmpty {
                parts.append(tail)
            }
            return parts
        }

        var paramBlock: Substring = ""
        if let fRange = lower.range(of: "create function") {
            var i = fRange.upperBound
            var inSingle = false
            var inBracket = false
            while i < lower.endIndex {
                let ch = lower[i]
                if inSingle {
                    if ch == "'" { inSingle = false }
                } else if inBracket {
                    if ch == "]" { inBracket = false }
                } else {
                    if ch == "'" { inSingle = true }
                    else if ch == "[" { inBracket = true }
                    else if ch == "(" { break }
                }
                i = lower.index(after: i)
            }
            guard i < lower.endIndex else { return [:] }

            var depth = 0
            var j = i
            repeat {
                let ch = lower[j]
                if ch == "(" { depth += 1 }
                else if ch == ")" { depth -= 1 }
                j = lower.index(after: j)
            } while j <= lower.endIndex && depth > 0

            let start = convertIndex(lower.index(after: i))
            let end = convertIndex(lower.index(before: j))
            paramBlock = text[start..<end]
        } else if let pRange = lower.range(of: "create procedure") ?? lower.range(of: "create proc") {
            let searchStart = pRange.upperBound
            guard let atIndex = lower[searchStart...].firstIndex(of: "@") else {
                return [:]
            }
            if let asRange = lower[searchStart...].range(of: "as"), asRange.lowerBound < atIndex {
                return [:]
            }

            var inSingle = false
            var inBracket = false
            var depth = 0
            var j = atIndex
            while j < lower.endIndex {
                let ch = lower[j]
                if inSingle {
                    if ch == "'" { inSingle = false }
                } else if inBracket {
                    if ch == "]" { inBracket = false }
                } else {
                    if ch == "'" { inSingle = true }
                    else if ch == "[" { inBracket = true }
                    else if ch == "(" { depth += 1 }
                    else if ch == ")" { depth = max(0, depth - 1) }
                    else if depth == 0,
                            j <= lower.index(lower.endIndex, offsetBy: -2) {
                        let ahead = lower[j...]
                        if ahead.hasPrefix("as ") || ahead.hasPrefix("as\n") || ahead.hasPrefix("as\r") || ahead.hasPrefix("as\t") {
                            break
                        }
                    }
                }
                j = lower.index(after: j)
            }

            let start = convertIndex(atIndex)
            let end = convertIndex(j)
            paramBlock = text[start..<end]
        } else {
            return [:]
        }

        let segments = splitTopLevel(byComma: paramBlock)
        var defaults: [String: (hasDefault: Bool, defaultValue: String?)] = [:]
        defaults.reserveCapacity(segments.count)

        for segmentRaw in segments {
            let segment = segmentRaw.trimmingCharacters(in: .whitespacesAndNewlines)
            guard let at = segment.firstIndex(of: "@") else { continue }
            let nameStart = at
            var nameEnd = segment.index(after: nameStart)
            while nameEnd < segment.endIndex {
                let c = segment[nameEnd]
                if c == " " || c == "\t" || c == "\n" || c == "=" || c == "," {
                    break
                }
                nameEnd = segment.index(after: nameEnd)
            }
            let key = segment[nameStart..<nameEnd].lowercased()
            guard let eq = segment.firstIndex(of: "=") else {
                defaults[String(key)] = (hasDefault: false, defaultValue: nil)
                continue
            }

            var defaultPart = segment[segment.index(after: eq)...].trimmingCharacters(in: .whitespacesAndNewlines)
            while let range = defaultPart.range(of: "[A-Za-z_]+$", options: .regularExpression) {
                let kw = defaultPart[range].lowercased()
                if kw == "output" || kw == "out" || kw == "readonly" {
                    defaultPart = defaultPart[..<range.lowerBound].trimmingCharacters(in: .whitespacesAndNewlines)
                } else {
                    break
                }
            }

            let cleaned = defaultPart.isEmpty ? nil : String(defaultPart)
            defaults[String(key)] = (hasDefault: cleaned != nil, defaultValue: cleaned)
        }

        return defaults
    }

    internal static func normalizeRoutineName(_ raw: String) -> String {
        let stripped = raw.split(separator: ";", maxSplits: 1, omittingEmptySubsequences: false).first.map(String.init) ?? raw
        return stripped.contains(".") ? String(stripped[stripped.index(after: stripped.firstIndex(of: ".")!)...]) : stripped
    }
}
