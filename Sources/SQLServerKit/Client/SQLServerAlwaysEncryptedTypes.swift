import Foundation

// MARK: - Always Encrypted Types

/// A column master key from `sys.column_master_keys`.
public struct ColumnMasterKeyInfo: Sendable, Hashable, Identifiable {
    public var id: String { name }
    public let name: String
    public let keyStoreProviderName: String
    public let keyPath: String
    public let allowEnclaveComputations: Bool
    public let createDate: String?

    public init(name: String, keyStoreProviderName: String, keyPath: String, allowEnclaveComputations: Bool = false, createDate: String? = nil) {
        self.name = name
        self.keyStoreProviderName = keyStoreProviderName
        self.keyPath = keyPath
        self.allowEnclaveComputations = allowEnclaveComputations
        self.createDate = createDate
    }
}

/// A column encryption key from `sys.column_encryption_keys`.
public struct ColumnEncryptionKeyInfo: Sendable, Hashable, Identifiable {
    public var id: String { name }
    public let name: String
    public let createDate: String?
    public let modifyDate: String?

    public init(name: String, createDate: String? = nil, modifyDate: String? = nil) {
        self.name = name
        self.createDate = createDate
        self.modifyDate = modifyDate
    }
}

/// A value entry for a column encryption key, linking it to a column master key.
public struct ColumnEncryptionKeyValueInfo: Sendable, Hashable {
    public let cekName: String
    public let cmkName: String
    public let encryptionAlgorithm: String

    public init(cekName: String, cmkName: String, encryptionAlgorithm: String) {
        self.cekName = cekName
        self.cmkName = cmkName
        self.encryptionAlgorithm = encryptionAlgorithm
    }
}

/// Information about an encrypted column.
public struct EncryptedColumnInfo: Sendable, Hashable, Identifiable {
    public var id: String { "\(schema).\(table).\(column)" }
    public let schema: String
    public let table: String
    public let column: String
    public let encryptionType: String
    public let cekName: String

    public init(schema: String, table: String, column: String, encryptionType: String, cekName: String) {
        self.schema = schema
        self.table = table
        self.column = column
        self.encryptionType = encryptionType
        self.cekName = cekName
    }
}
