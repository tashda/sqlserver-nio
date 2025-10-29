import Foundation

public struct SQLServerSessionStateInfo: Sendable {
    public let sequenceNumber: Int
    public let recoverable: Bool
    public let entries: [UInt8: [UInt8]] // id -> raw bytes
}

public enum SQLServerSensitivityRank: Int, Sendable { case notDefined = -1, low = 0, medium = 1, high = 2, critical = 3 }

public struct SQLServerSensitivityLabel: Sendable { public let name: String; public let id: String }
public struct SQLServerInformationType: Sendable { public let name: String; public let id: String }
public struct SQLServerSensitivityProperty: Sendable {
    public let label: SQLServerSensitivityLabel?
    public let informationType: SQLServerInformationType?
    public let rank: SQLServerSensitivityRank?
}
public struct SQLServerColumnSensitivity: Sendable { public let properties: [SQLServerSensitivityProperty] }
public struct SQLServerSensitivityClassification: Sendable {
    public let labels: [SQLServerSensitivityLabel]
    public let informationTypes: [SQLServerInformationType]
    public let columns: [SQLServerColumnSensitivity]
    public let rank: SQLServerSensitivityRank?
}

// Internal decoder utilities so tests can validate parsing directly
enum SQLServerSessionAndClassificationDecoder {
    static func decodeSessionState(from bytes: [UInt8]) -> SQLServerSessionStateInfo? {
        guard !bytes.isEmpty else { return nil }
        var idx = 0
        func readUInt32() -> UInt32? {
            guard idx + 4 <= bytes.count else { return nil }
            // Manual little-endian assembly to avoid misaligned raw loads
            let b0 = UInt32(bytes[idx])
            let b1 = UInt32(bytes[idx + 1]) << 8
            let b2 = UInt32(bytes[idx + 2]) << 16
            let b3 = UInt32(bytes[idx + 3]) << 24
            idx += 4
            return b0 | b1 | b2 | b3
        }
        func readUInt8() -> UInt8? { guard idx + 1 <= bytes.count else { return nil }; defer { idx += 1 }; return bytes[idx] }
        func readVarLen() -> Int? {
            guard let b = readUInt8() else { return nil }
            if b < 0xFF { return Int(b) }
            guard let v = readUInt32() else { return nil }
            return Int(v)
        }
        guard let seq = readUInt32(), let status = readUInt8() else { return nil }
        var map: [UInt8: [UInt8]] = [:]
        while idx < bytes.count {
            guard let sid = readUInt8(), let len = readVarLen() else { break }
            guard idx + len <= bytes.count else { break }
            let slice = Array(bytes[idx..<(idx+len)])
            idx += len
            map[sid] = slice
        }
        return SQLServerSessionStateInfo(sequenceNumber: Int(seq), recoverable: (status & 0x01) != 0, entries: map)
    }

    static func decodeSensitivityClassification(from bytes: [UInt8]) -> SQLServerSensitivityClassification? {
        guard !bytes.isEmpty else { return nil }
        var idx = 0
        func readUInt16() -> UInt16? {
            guard idx + 2 <= bytes.count else { return nil }
            // Manual little-endian assembly to avoid misaligned raw loads
            let b0 = UInt16(bytes[idx])
            let b1 = UInt16(bytes[idx + 1]) << 8
            idx += 2
            return b0 | b1
        }
        func readUInt32() -> UInt32? {
            guard idx + 4 <= bytes.count else { return nil }
            // Manual little-endian assembly to avoid misaligned raw loads
            let b0 = UInt32(bytes[idx])
            let b1 = UInt32(bytes[idx + 1]) << 8
            let b2 = UInt32(bytes[idx + 2]) << 16
            let b3 = UInt32(bytes[idx + 3]) << 24
            idx += 4
            return b0 | b1 | b2 | b3
        }
        func readBVarChar() -> String? {
            guard idx < bytes.count else { return nil }
            let len = Int(bytes[idx]); idx += 1
            guard len >= 0, idx + (len * 2) <= bytes.count else { return nil }
            let slice = Data(bytes[idx..<(idx + len*2)])
            idx += len*2
            return String(data: slice, encoding: .utf16LittleEndian)
        }
        guard let labelCount = readUInt16() else { return nil }
        var labels: [SQLServerSensitivityLabel] = []
        for _ in 0..<labelCount { if let name = readBVarChar(), let id = readBVarChar() { labels.append(.init(name: name, id: id)) } }
        guard let infoCount = readUInt16() else { return nil }
        var infos: [SQLServerInformationType] = []
        for _ in 0..<infoCount { if let name = readBVarChar(), let id = readBVarChar() { infos.append(.init(name: name, id: id)) } }
        // Try to read optional global rank
        var globalRank: SQLServerSensitivityRank? = nil
        if let maybeRank = readUInt32(), let rank = SQLServerSensitivityRank(rawValue: Int(Int32(bitPattern: maybeRank))) {
            globalRank = rank
        } else {
            if idx >= 4 { idx -= 4 }
        }
        guard let columnCount = readUInt16() else { return nil }
        var columns: [SQLServerColumnSensitivity] = []
        columns.reserveCapacity(Int(columnCount))
        for _ in 0..<columnCount {
            guard let propCount = readUInt16() else { return nil }
            var props: [SQLServerSensitivityProperty] = []
            for _ in 0..<propCount {
                guard let labelIdx = readUInt16(), let infoIdx = readUInt16() else { return nil }
                var rank: SQLServerSensitivityRank? = nil
                if idx + 4 <= bytes.count, let r = readUInt32(), let sr = SQLServerSensitivityRank(rawValue: Int(Int32(bitPattern: r))) { rank = sr } else { if idx >= 4 { idx -= 4 } }
                let label = (labelIdx == 0xFFFF) ? nil : (labelIdx < labels.count ? labels[Int(labelIdx)] : nil)
                let info = (infoIdx == 0xFFFF) ? nil : (infoIdx < infos.count ? infos[Int(infoIdx)] : nil)
                props.append(.init(label: label, informationType: info, rank: rank))
            }
            columns.append(.init(properties: props))
        }
        return .init(labels: labels, informationTypes: infos, columns: columns, rank: globalRank)
    }
}

extension SQLServerConnection {
    public func decodeLastSessionState() -> SQLServerSessionStateInfo? {
        let bytes = underlying.snapshotSessionStatePayload()
        return SQLServerSessionAndClassificationDecoder.decodeSessionState(from: bytes)
    }
    

    public func decodeLastSensitivityClassification() -> SQLServerSensitivityClassification? {
        let bytes = underlying.snapshotDataClassificationPayload()
        return SQLServerSessionAndClassificationDecoder.decodeSensitivityClassification(from: bytes)
    }
}
