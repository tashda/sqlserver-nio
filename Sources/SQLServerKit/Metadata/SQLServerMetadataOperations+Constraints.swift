import Foundation
import NIO
import SQLServerTDS

extension SQLServerMetadataOperations {
    // MARK: - Primary Keys

    public func listPrimaryKeys(
        database: String? = nil,
        schema: String? = nil,
        table: String? = nil
    ) -> EventLoopFuture<[KeyConstraintMetadata]> {
        fetchKeyConstraints(type: .primaryKey, database: database, schema: schema, table: table)
    }

    public func listPrimaryKeysFromCatalog(
        database: String? = nil,
        schema: String? = nil,
        table: String? = nil
    ) -> EventLoopFuture<[KeyConstraintMetadata]> {
        fetchKeyConstraints(type: .primaryKey, database: database, schema: schema, table: table)
    }

    private func listPrimaryKeysForSingleTable(
        database: String?,
        schema: String?,
        table: String
    ) -> EventLoopFuture<[KeyConstraintMetadata]> {
        var parameters: [String] = ["@table_name = N'\(SQLServerSQL.escapeLiteral(table))'"]
        if let schema { parameters.append("@table_owner = N'\(SQLServerSQL.escapeLiteral(schema))'") }
        if let database { parameters.append("@table_qualifier = N'\(SQLServerSQL.escapeLiteral(database))'") }
        
        let sql = "SET NOCOUNT ON; EXEC sp_pkeys \(parameters.joined(separator: ", "));"

        return fetchPrimaryKeyClusterInfo(database: database, schema: schema, table: table).flatMap { clusterInfo in
            self.queryExecutor(sql).map { rows in
                var grouped: [String: (schema: String, table: String, name: String, columns: [KeyColumnMetadata])] = [:]
                for row in rows {
                    guard let sName = row.column("TABLE_OWNER")?.string, let tName = row.column("TABLE_NAME")?.string, let cName = row.column("COLUMN_NAME")?.string else { continue }
                    let keyName = row.column("PK_NAME")?.string ?? "PRIMARY"
                    let key = "\(sName)|\(tName)|\(keyName)"
                    var entry = grouped[key] ?? (schema: sName, table: tName, name: keyName, columns: [])
                    let ordinal = row.column("KEY_SEQ")?.int ?? (entry.columns.count + 1)
                    if !entry.columns.contains(where: { $0.column.caseInsensitiveCompare(cName) == .orderedSame && $0.ordinal == ordinal }) {
                        entry.columns.append(KeyColumnMetadata(column: cName, ordinal: ordinal, isDescending: false))
                    }
                    grouped[key] = entry
                }
                return grouped.values.sorted { $0.name < $1.name }.map { e in
                    let isClustered = clusterInfo["\(e.schema)|\(e.table)|\(e.name)"] ?? false
                    return KeyConstraintMetadata(schema: e.schema, table: e.table, name: e.name, type: .primaryKey, isClustered: isClustered, columns: e.columns.sorted { $0.ordinal < $1.ordinal })
                }
            }
        }
    }

    // MARK: - Unique Constraints

    public func listUniqueConstraints(
        database: String? = nil,
        schema: String? = nil,
        table: String? = nil
    ) -> EventLoopFuture<[KeyConstraintMetadata]> {
        fetchKeyConstraints(type: .unique, database: database, schema: schema, table: table)
    }

    private func fetchKeyConstraints(
        type: KeyConstraintMetadata.ConstraintType,
        database: String?,
        schema: String?,
        table: String?
    ) -> EventLoopFuture<[KeyConstraintMetadata]> {
        var sql = """
        SELECT schema_name = s.name, table_name = t.name, constraint_name = kc.name,
               is_clustered = CASE WHEN i.type = 1 THEN 1 ELSE 0 END,
               column_name = c.name, ordinal = ic.key_ordinal, is_descending = ic.is_descending_key
        FROM \(qualified(database, object: "sys.key_constraints")) AS kc
        JOIN \(qualified(database, object: "sys.tables")) AS t ON kc.parent_object_id = t.object_id
        JOIN \(qualified(database, object: "sys.schemas")) AS s ON t.schema_id = s.schema_id
        JOIN \(qualified(database, object: "sys.indexes")) AS i ON kc.parent_object_id = i.object_id AND kc.unique_index_id = i.index_id
        JOIN \(qualified(database, object: "sys.index_columns")) AS ic ON kc.parent_object_id = ic.object_id AND kc.unique_index_id = ic.index_id
        JOIN \(qualified(database, object: "sys.columns")) AS c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE kc.type = '\(type == .primaryKey ? "PK" : "UQ")'
        """
        var predicates: [String] = []
        if let schema { predicates.append("s.name = N'\(SQLServerSQL.escapeLiteral(schema))'") }
        if let table { predicates.append("t.name = N'\(SQLServerSQL.escapeLiteral(table))'") }
        if !predicates.isEmpty { sql += " AND " + predicates.joined(separator: " AND ") }
        sql += " ORDER BY s.name, t.name, kc.name, ic.key_ordinal;"

        return queryExecutor(sql).map { rows in
            var grouped: [String: (schema: String, table: String, name: String, isClustered: Bool, columns: [KeyColumnMetadata])] = [:]
            for row in rows {
                guard let sName = row.column("schema_name")?.string, let tName = row.column("table_name")?.string,
                      let cName = row.column("constraint_name")?.string, let colName = row.column("column_name")?.string,
                      let ordinal = row.column("ordinal")?.int else { continue }
                let key = "\(sName)|\(tName)|\(cName)"
                var entry = grouped[key] ?? (schema: sName, table: tName, name: cName, isClustered: (row.column("is_clustered")?.int ?? 0) != 0, columns: [])
                if !entry.columns.contains(where: { $0.column.caseInsensitiveCompare(colName) == .orderedSame && $0.ordinal == ordinal }) {
                    entry.columns.append(KeyColumnMetadata(column: colName, ordinal: ordinal, isDescending: (row.column("is_descending")?.int ?? 0) != 0))
                }
                grouped[key] = entry
            }
            return grouped.values.map { e in
                KeyConstraintMetadata(schema: e.schema, table: e.table, name: e.name, type: type, isClustered: e.isClustered, columns: e.columns.sorted { $0.ordinal < $1.ordinal })
            }.sorted { $0.name < $1.name }
        }
    }

    private func fetchPrimaryKeyClusterInfo(database: String?, schema: String?, table: String?) -> EventLoopFuture<[String: Bool]> {
        var sql = "SELECT s.name as schema_name, t.name as table_name, kc.name as constraint_name, is_clustered = CASE WHEN i.type = 1 THEN 1 ELSE 0 END FROM \(qualified(database, object: "sys.key_constraints")) kc JOIN \(qualified(database, object: "sys.tables")) t ON kc.parent_object_id = t.object_id JOIN \(qualified(database, object: "sys.schemas")) s ON t.schema_id = s.schema_id JOIN \(qualified(database, object: "sys.indexes")) i ON kc.parent_object_id = i.object_id AND kc.unique_index_id = i.index_id WHERE kc.type = 'PK'"
        if let schema { sql += " AND s.name = N'\(SQLServerSQL.escapeLiteral(schema))'" }
        if let table { sql += " AND t.name = N'\(SQLServerSQL.escapeLiteral(table))'" }
        return queryExecutor(sql).map { rows in
            var info: [String: Bool] = [:]
            for row in rows {
                if let s = row.column("schema_name")?.string, let t = row.column("table_name")?.string, let c = row.column("constraint_name")?.string {
                    info["\(s)|\(t)|\(c)"] = (row.column("is_clustered")?.int ?? 0) != 0
                }
            }
            return info
        }
    }

    // MARK: - Indexes

    internal func listIndexes(database: String? = nil, schema: String, table: String) -> EventLoopFuture<[IndexMetadata]> {
        let sql = """
        SELECT
            schema_name = s.name,
            table_name = o.name,
            index_name = i.name,
            is_unique = i.is_unique,
            index_type = i.type,
            is_primary_key = i.is_primary_key,
            is_unique_constraint = i.is_unique_constraint,
            filter_definition = i.filter_definition,
            column_name = c.name,
            ordinal = ic.index_column_id,
            key_ordinal = ic.key_ordinal,
            is_descending = ic.is_descending_key,
            is_included = ic.is_included_column
        FROM \(qualified(database, object: "sys.indexes")) AS i
        JOIN \(qualified(database, object: "sys.objects")) AS o ON i.object_id = o.object_id
        JOIN \(qualified(database, object: "sys.schemas")) AS s ON o.schema_id = s.schema_id
        LEFT JOIN \(qualified(database, object: "sys.index_columns")) AS ic
            ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        LEFT JOIN \(qualified(database, object: "sys.columns")) AS c
            ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE s.name = N'\(SQLServerSQL.escapeLiteral(schema))'
          AND o.name = N'\(SQLServerSQL.escapeLiteral(table))'
          AND o.type IN ('U', 'V')
          AND i.index_id > 0
          AND i.is_hypothetical = 0
        ORDER BY s.name, o.name, i.name, ic.key_ordinal, ic.index_column_id;
        """
        return queryExecutor(sql).map { rows in
            var grouped: [String: (schema: String, table: String, name: String, isUnique: Bool, isClustered: Bool, isPrimaryKey: Bool, isUniqueConstraint: Bool, filter: String?, cols: [IndexColumnMetadata])] = [:]
            for row in rows {
                guard let sName = row.column("schema_name")?.string,
                      let tName = row.column("table_name")?.string,
                      let iName = row.column("index_name")?.string else { continue }
                let key = "\(sName)|\(tName)|\(iName)"
                let typeCode = row.column("index_type")?.int ?? 0
                var entry = grouped[key] ?? (
                    schema: sName,
                    table: tName,
                    name: iName,
                    isUnique: (row.column("is_unique")?.int ?? 0) != 0,
                    isClustered: typeCode == 1 || typeCode == 5,
                    isPrimaryKey: (row.column("is_primary_key")?.int ?? 0) != 0,
                    isUniqueConstraint: (row.column("is_unique_constraint")?.int ?? 0) != 0,
                    filter: row.column("filter_definition")?.string,
                    cols: []
                )
                if let cName = row.column("column_name")?.string {
                    let ordinal = row.column("key_ordinal")?.int ?? row.column("ordinal")?.int ?? (entry.cols.count + 1)
                    entry.cols.append(IndexColumnMetadata(
                        column: cName,
                        ordinal: ordinal,
                        isDescending: (row.column("is_descending")?.int ?? 0) != 0,
                        isIncluded: (row.column("is_included")?.int ?? 0) != 0
                    ))
                }
                grouped[key] = entry
            }
            return grouped.values.map { e in
                IndexMetadata(
                    schema: e.schema,
                    table: e.table,
                    name: e.name,
                    isUnique: e.isUnique,
                    isClustered: e.isClustered,
                    isPrimaryKey: e.isPrimaryKey,
                    isUniqueConstraint: e.isUniqueConstraint,
                    filterDefinition: e.filter,
                    columns: e.cols.sorted { lhs, rhs in
                        if lhs.ordinal != rhs.ordinal { return lhs.ordinal < rhs.ordinal }
                        return lhs.column < rhs.column
                    }
                )
            }.sorted { $0.name < $1.name }
        }
    }

    // MARK: - Foreign Keys

    internal func listForeignKeys(database: String? = nil, schema: String, table: String) -> EventLoopFuture<[ForeignKeyMetadata]> {
        let sql = """
        SELECT
            fk_schema = fs.name,
            fk_table = ft.name,
            fk_name = fk.name,
            pk_schema = ps.name,
            pk_table = pt.name,
            delete_rule = fk.delete_referential_action,
            update_rule = fk.update_referential_action,
            ordinal = fkc.constraint_column_id,
            fk_column = fc.name,
            pk_column = pc.name
        FROM \(qualified(database, object: "sys.foreign_keys")) AS fk
        JOIN \(qualified(database, object: "sys.tables")) AS ft ON fk.parent_object_id = ft.object_id
        JOIN \(qualified(database, object: "sys.schemas")) AS fs ON ft.schema_id = fs.schema_id
        JOIN \(qualified(database, object: "sys.tables")) AS pt ON fk.referenced_object_id = pt.object_id
        JOIN \(qualified(database, object: "sys.schemas")) AS ps ON pt.schema_id = ps.schema_id
        JOIN \(qualified(database, object: "sys.foreign_key_columns")) AS fkc ON fk.object_id = fkc.constraint_object_id
        JOIN \(qualified(database, object: "sys.columns")) AS fc ON fkc.parent_object_id = fc.object_id AND fkc.parent_column_id = fc.column_id
        JOIN \(qualified(database, object: "sys.columns")) AS pc ON fkc.referenced_object_id = pc.object_id AND fkc.referenced_column_id = pc.column_id
        WHERE fs.name = N'\(SQLServerSQL.escapeLiteral(schema))'
          AND ft.name = N'\(SQLServerSQL.escapeLiteral(table))'
        ORDER BY fs.name, ft.name, fk.name, fkc.constraint_column_id;
        """
        return queryExecutor(sql).map { rows in
            var grouped: [String: (schema: String, table: String, name: String, refSchema: String, refTable: String, del: Int, upd: Int, cols: [ForeignKeyColumnMetadata])] = [:]
            for row in rows {
                guard let fs = row.column("fk_schema")?.string,
                      let ft = row.column("fk_table")?.string,
                      let n = row.column("fk_name")?.string,
                      let ps = row.column("pk_schema")?.string,
                      let pt = row.column("pk_table")?.string else { continue }
                let key = "\(fs)|\(ft)|\(n)"
                var entry = grouped[key] ?? (schema: fs, table: ft, name: n, refSchema: ps, refTable: pt, del: row.column("delete_rule")?.int ?? 1, upd: row.column("update_rule")?.int ?? 1, cols: [])
                if let pc = row.column("fk_column")?.string, let rc = row.column("pk_column")?.string, let ord = row.column("ordinal")?.int {
                    entry.cols.append(ForeignKeyColumnMetadata(parentColumn: pc, referencedColumn: rc, ordinal: ord))
                }
                grouped[key] = entry
            }
            return grouped.values.map { e in
                ForeignKeyMetadata(schema: e.schema, table: e.table, name: e.name, referencedSchema: e.refSchema, referencedTable: e.refTable, deleteAction: ForeignKeyMetadata.mapAction(e.del), updateAction: ForeignKeyMetadata.mapAction(e.upd), columns: e.cols.sorted { $0.ordinal < $1.ordinal })
            }.sorted { $0.name < $1.name }
        }
    }

    // MARK: - Dependencies

    internal func listDependencies(database: String? = nil, schema: String, object: String) -> EventLoopFuture<[DependencyMetadata]> {
        let sql = "WITH target AS (SELECT o.object_id FROM \(qualified(database, object: "sys.objects")) o JOIN \(qualified(database, object: "sys.schemas")) s ON o.schema_id = s.schema_id WHERE s.name = N'\(SQLServerSQL.escapeLiteral(schema))' AND o.name = N'\(SQLServerSQL.escapeLiteral(object))') SELECT rs.name as referencing_schema, ro.name as referencing_object, ro.type_desc as referencing_type, sed.is_schema_bound_reference as is_schema_bound FROM target JOIN \(qualified(database, object: "sys.sql_expression_dependencies")) sed ON sed.referenced_id = target.object_id JOIN \(qualified(database, object: "sys.objects")) ro ON sed.referencing_id = ro.object_id JOIN \(qualified(database, object: "sys.schemas")) rs ON ro.schema_id = rs.schema_id WHERE sed.referenced_minor_id = 0 ORDER BY rs.name, ro.name;"
        return queryExecutor(sql).map { rows in
            rows.compactMap { row in
                guard let s = row.column("referencing_schema")?.string, let o = row.column("referencing_object")?.string, let t = row.column("referencing_type")?.string else { return nil }
                return DependencyMetadata(referencingSchema: s, referencingObject: o, referencingType: t, isSchemaBound: row.column("is_schema_bound")?.bool ?? false)
            }
        }
    }

    // MARK: - Bidirectional Dependencies

    internal func objectDependencies(database: String? = nil, schema: String, name: String) -> EventLoopFuture<[SQLServerObjectDependency]> {
        let escapedSchema = SQLServerSQL.escapeLiteral(schema)
        let escapedName = SQLServerSQL.escapeLiteral(name)
        let sql = """
        SELECT
            referencing_entity_name = OBJECT_NAME(d.referencing_id),
            referencing_type = o1.type_desc,
            referenced_entity_name = COALESCE(d.referenced_entity_name, OBJECT_NAME(d.referenced_id)),
            referenced_type = COALESCE(o2.type_desc, d.referenced_class_desc)
        FROM \(qualified(database, object: "sys.sql_expression_dependencies")) d
        LEFT JOIN \(qualified(database, object: "sys.objects")) o1 ON d.referencing_id = o1.object_id
        LEFT JOIN \(qualified(database, object: "sys.objects")) o2 ON d.referenced_id = o2.object_id
        WHERE d.referencing_id = OBJECT_ID(N'\(escapedSchema).\(escapedName)')
           OR d.referenced_id = OBJECT_ID(N'\(escapedSchema).\(escapedName)');
        """
        return queryExecutor(sql).map { rows in
            rows.compactMap { row in
                guard let referencingName = row.column("referencing_entity_name")?.string,
                      let referencingType = row.column("referencing_type")?.string,
                      let referencedName = row.column("referenced_entity_name")?.string,
                      let referencedType = row.column("referenced_type")?.string
                else { return nil }
                return SQLServerObjectDependency(
                    referencingName: referencingName,
                    referencingType: referencingType,
                    referencedName: referencedName,
                    referencedType: referencedType
                )
            }
        }
    }

    // MARK: - Table Properties

    internal func tableProperties(database: String? = nil, schema: String, table: String) -> EventLoopFuture<SQLServerTableProperties> {
        let escapedSchema = SQLServerSQL.escapeLiteral(schema)
        let escapedTable = SQLServerSQL.escapeLiteral(table)

        // Query create/modify dates from sys.objects
        let datesSql = """
        SELECT o.create_date, o.modify_date
        FROM \(qualified(database, object: "sys.objects")) o
        JOIN \(qualified(database, object: "sys.schemas")) s ON o.schema_id = s.schema_id
        WHERE s.name = N'\(escapedSchema)' AND o.name = N'\(escapedTable)';
        """

        // Query space usage from sys.dm_db_partition_stats and sys.allocation_units
        // This avoids sp_spaceused which returns multiple result sets
        let spaceSql = """
        SELECT
            SUM(p.rows) AS row_count,
            SUM(a.total_pages) * 8 AS reserved_kb,
            SUM(a.data_pages) * 8 AS data_kb,
            SUM(CASE WHEN a.type <> 1 THEN a.used_pages ELSE 0 END) * 8 AS index_kb,
            (SUM(a.total_pages) - SUM(a.used_pages)) * 8 AS unused_kb
        FROM \(qualified(database, object: "sys.partitions")) p
        JOIN \(qualified(database, object: "sys.allocation_units")) a ON p.partition_id = a.container_id
        JOIN \(qualified(database, object: "sys.objects")) o ON p.object_id = o.object_id
        JOIN \(qualified(database, object: "sys.schemas")) s ON o.schema_id = s.schema_id
        WHERE s.name = N'\(escapedSchema)' AND o.name = N'\(escapedTable)' AND p.index_id IN (0, 1);
        """

        return queryExecutor(datesSql).flatMap { dateRows in
            let createDate = dateRows.first?.column("create_date")?.date
            let modifyDate = dateRows.first?.column("modify_date")?.date
            return self.queryExecutor(spaceSql).map { spaceRows in
                let row = spaceRows.first
                return SQLServerTableProperties(
                    rowCount: row?.column("row_count")?.int64 ?? 0,
                    reservedKB: row?.column("reserved_kb")?.int64 ?? 0,
                    dataKB: row?.column("data_kb")?.int64 ?? 0,
                    indexKB: row?.column("index_kb")?.int64 ?? 0,
                    unusedKB: row?.column("unused_kb")?.int64 ?? 0,
                    createDate: createDate,
                    modifyDate: modifyDate
                )
            }
        }
    }

    // MARK: - Object Definition (raw string)

    internal func objectDefinitionString(database: String? = nil, schema: String, name: String) -> EventLoopFuture<String?> {
        let escapedSchema = SQLServerSQL.escapeLiteral(schema)
        let escapedName = SQLServerSQL.escapeLiteral(name)
        let sql = "SELECT OBJECT_DEFINITION(OBJECT_ID(N'\(escapedSchema).\(escapedName)')) AS definition;"
        return queryExecutor(sql).map { rows in
            rows.first?.column("definition")?.string
        }
    }
}
