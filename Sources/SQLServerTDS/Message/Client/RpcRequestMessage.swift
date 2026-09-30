import Foundation
import NIO

extension TDSMessages {
    public struct RpcParameter: Sendable {
        public enum Direction: Sendable {
            case `in`
            case out
            case `inout`
        }

        public let name: String
        public let data: TDSData?
        public let direction: Direction

        public init(name: String, data: TDSData?, direction: Direction = .in) {
            self.name = name
            self.data = data
            self.direction = direction
        }
    }

    public struct RpcRequestMessage: TDSMessagePayload, Sendable {
        public static let packetType: TDSPacket.HeaderType = .rpc

        public let procedureName: String
        public let parameters: [RpcParameter]
        public let transactionDescriptor: [UInt8]
        public let outstandingRequestCount: UInt32

        public init(
            procedureName: String,
            parameters: [RpcParameter],
            transactionDescriptor: [UInt8] = [0, 0, 0, 0, 0, 0, 0, 0],
            outstandingRequestCount: UInt32 = 1
        ) {
            self.procedureName = procedureName
            self.parameters = parameters
            self.transactionDescriptor = transactionDescriptor
            self.outstandingRequestCount = outstandingRequestCount
        }

        public func serialize(into buffer: inout ByteBuffer) throws {
            SQLServerTDS.TDSMessage.serializeAllHeaders(
                &buffer,
                transactionDescriptor: transactionDescriptor,
                outstandingRequestCount: outstandingRequestCount
            )

            let procChars = Array(procedureName.utf16)
            guard procChars.count <= 0xFFFF else {
                throw TDSError.protocolError("RPC procedure name too long")
            }
            buffer.writeInteger(UInt16(procChars.count), endianness: .little)
            for ch in procChars {
                buffer.writeInteger(ch, endianness: .little)
            }
            buffer.writeInteger(UInt16(0), endianness: .little)

            let useParamNameASCII = ProcessInfo.processInfo.environment["TDS_RPC_PARAMNAME_ASCII"] == "1"
            let includeDecimalScaleInTypeInfo = ProcessInfo.processInfo.environment["TDS_RPC_DEC_TYPEINFO_SCALE"] == "1"

            for parameter in parameters {
                let rawName = parameter.name.hasPrefix("@") ? String(parameter.name.dropFirst()) : parameter.name
                let parameterName = "@" + rawName

                if useParamNameASCII, let ascii = parameterName.data(using: .ascii) {
                    guard ascii.count <= 0xFF else {
                        throw TDSError.protocolError("RPC parameter name too long: \(parameter.name)")
                    }
                    buffer.writeInteger(UInt8(ascii.count))
                    buffer.writeBytes(ascii)
                } else {
                    let nameChars = Array(parameterName.utf16)
                    guard nameChars.count <= 0xFF else {
                        throw TDSError.protocolError("RPC parameter name too long: \(parameter.name)")
                    }
                    buffer.writeInteger(UInt8(nameChars.count))
                    for ch in nameChars {
                        buffer.writeInteger(ch, endianness: .little)
                    }
                }

                let status: UInt8 = (parameter.direction == .out || parameter.direction == .inout) ? 0x01 : 0x00
                buffer.writeInteger(status)

                try writeTypeInfoAndValue(
                    for: parameter,
                    into: &buffer,
                    includeDecimalScaleInTypeInfo: includeDecimalScaleInTypeInfo
                )
            }
        }

        private func writeTypeInfoAndValue(
            for parameter: RpcParameter,
            into buffer: inout ByteBuffer,
            includeDecimalScaleInTypeInfo: Bool
        ) throws {
            guard let data = parameter.data else {
                // Provide a generic nullable string placeholder when the caller only
                // needs an OUTPUT slot and the concrete server-side type is declared
                // by the stored procedure parameter itself.
                buffer.writeInteger(TDSDataType.nvarchar.rawValue)
                buffer.writeInteger(UInt16(8000), endianness: .little)
                buffer.writeBytes([0, 0, 0, 0, 0])
                buffer.writeInteger(UInt16(0xFFFF), endianness: .little)
                return
            }

            switch data.metadata.dataType {
            case .decimal, .numeric, .decimalLegacy, .numericLegacy:
                buffer.writeInteger(data.metadata.dataType.rawValue)
                let scale = data.metadata.scale
                buffer.writeInteger(UInt8(0x11))
                buffer.writeInteger(UInt8(38))
                if includeDecimalScaleInTypeInfo {
                    buffer.writeInteger(scale)
                }

                if parameter.direction != .in {
                    if !includeDecimalScaleInTypeInfo {
                        buffer.writeInteger(scale)
                    }
                    buffer.writeInteger(UInt8(0))
                    return
                }

                guard var value = data.value else {
                    if !includeDecimalScaleInTypeInfo {
                        buffer.writeInteger(scale)
                    }
                    buffer.writeInteger(UInt8(0))
                    return
                }

                guard let sign = value.readInteger(as: UInt8.self) else {
                    if !includeDecimalScaleInTypeInfo {
                        buffer.writeInteger(scale)
                    }
                    buffer.writeInteger(UInt8(0))
                    return
                }

                let magnitude = value.readBytes(length: value.readableBytes) ?? []
                var magnitudeLength = magnitude.count
                while magnitudeLength > 1, magnitude[magnitudeLength - 1] == 0 {
                    magnitudeLength -= 1
                }

                if !includeDecimalScaleInTypeInfo {
                    buffer.writeInteger(scale)
                }
                buffer.writeInteger(UInt8(magnitudeLength + 1))
                buffer.writeInteger(sign)
                buffer.writeBytes(magnitude.prefix(magnitudeLength))

            case .tinyInt, .smallInt, .int, .bigInt, .intn:
                buffer.writeInteger(TDSDataType.intn.rawValue)
                let maxLength: UInt8 = {
                    switch data.metadata.dataType {
                    case .tinyInt: return 1
                    case .smallInt: return 2
                    case .int: return 4
                    case .bigInt: return 8
                    case .intn:
                        return UInt8(min(max(data.value?.readableBytes ?? 4, 1), 8))
                    default:
                        return 4
                    }
                }()
                buffer.writeInteger(maxLength)

                if parameter.direction != .in {
                    buffer.writeInteger(UInt8(0))
                    return
                }

                guard var value = data.value else {
                    buffer.writeInteger(UInt8(0))
                    return
                }

                let valueLength = UInt8(value.readableBytes)
                buffer.writeInteger(valueLength)
                if valueLength > 0, let bytes = value.readBytes(length: Int(valueLength)) {
                    buffer.writeBytes(bytes)
                }

            case .bit, .bitn:
                buffer.writeInteger(TDSDataType.bitn.rawValue)
                buffer.writeInteger(UInt8(1))
                if parameter.direction != .in {
                    buffer.writeInteger(UInt8(0))
                } else if let value = data.value?.getInteger(at: data.value!.readerIndex, as: UInt8.self) {
                    buffer.writeInteger(UInt8(1))
                    buffer.writeInteger(value)
                } else {
                    buffer.writeInteger(UInt8(0))
                }

            case .varchar, .char:
                let valueBytes = data.value.flatMap { $0.getBytes(at: $0.readerIndex, length: $0.readableBytes) }
                if data.metadata.collation.isEmpty {
                    // A Swift string (TDSData(string:) holds UTF-8 and has no collation): send it as
                    // nvarchar in UTF-16LE, as Microsoft's drivers do by default. Guessing from the
                    // byte count sent every even-length string as garbage.
                    let utf16 = valueBytes.map { bytes -> [UInt8] in
                        String(decoding: bytes, as: UTF8.self).utf16.flatMap { [UInt8($0 & 0xFF), UInt8($0 >> 8)] }
                    }
                    Self.writeVariableLength(
                        type: TDSDataType.nvarchar.rawValue,
                        collation: collationBytes(from: data.metadata.collation),
                        value: parameter.direction != .in ? nil : utf16,
                        into: &buffer
                    )
                } else {
                    // Bytes from a server column, in its collation's code page.
                    Self.writeVariableLength(
                        type: data.metadata.dataType.rawValue,
                        collation: collationBytes(from: data.metadata.collation),
                        value: parameter.direction != .in ? nil : valueBytes,
                        into: &buffer
                    )
                }

            case .nvarchar, .nchar:
                Self.writeVariableLength(
                    type: data.metadata.dataType.rawValue,
                    collation: collationBytes(from: data.metadata.collation),
                    value: parameter.direction != .in ? nil : data.value.flatMap { $0.getBytes(at: $0.readerIndex, length: $0.readableBytes) },
                    into: &buffer
                )

            case .varbinary, .binary:
                Self.writeVariableLength(
                    type: data.metadata.dataType.rawValue,
                    collation: nil,
                    value: parameter.direction != .in ? nil : data.value.flatMap { $0.getBytes(at: $0.readerIndex, length: $0.readableBytes) },
                    into: &buffer
                )

            case .guid:
                buffer.writeInteger(TDSDataType.guid.rawValue)
                buffer.writeInteger(UInt8(16))
                if parameter.direction != .in {
                    buffer.writeBytes([UInt8](repeating: 0, count: 16))
                } else if let value = data.value, let bytes = value.getBytes(at: value.readerIndex, length: value.readableBytes), bytes.count == 16 {
                    buffer.writeBytes(bytes)
                } else {
                    buffer.writeBytes([UInt8](repeating: 0, count: 16))
                }

            default:
                throw TDSError.protocolError("Unsupported RPC param type: \(data.metadata.dataType)")
            }
        }

        private func collationBytes(from source: [UInt8]) -> [UInt8] {
            source.count == 5 ? source : [0, 0, 0, 0, 0]
        }
    }
}

extension TDSMessages.RpcRequestMessage {
    /// A string or binary parameter: USHORTLEN_TYPE up to 8000 bytes (`nvarchar(4000)`,
    /// `varchar(8000)`, `varbinary(8000)`), and the MAX type with PLP chunks above that, as
    /// Microsoft's drivers send it. `nil` sends NULL (output parameters are sent as NULL).
    static func writeVariableLength(type: UInt8, collation: [UInt8]?, value: [UInt8]?, into buffer: inout ByteBuffer) {
        let isMax = (value?.count ?? 0) > 8000
        // Fixed-length types have no MAX form: a longer value goes as the varying type.
        let maxType: [UInt8: UInt8] = [
            TDSDataType.char.rawValue: TDSDataType.varchar.rawValue,
            TDSDataType.nchar.rawValue: TDSDataType.nvarchar.rawValue,
            TDSDataType.binary.rawValue: TDSDataType.varbinary.rawValue,
        ]
        buffer.writeInteger(isMax ? (maxType[type] ?? type) : type)
        // TYPE_INFO: maximum length (0xFFFF means MAX), then the collation for character types.
        buffer.writeInteger(isMax ? UInt16(0xFFFF) : UInt16(8000), endianness: .little)
        if let collation { buffer.writeBytes(collation) }
        guard let value else {
            buffer.writeInteger(UInt16(0xFFFF), endianness: .little) // CHARBIN_NULL
            return
        }
        guard isMax else {
            buffer.writeInteger(UInt16(value.count), endianness: .little)
            buffer.writeBytes(value)
            return
        }
        // PLP: total length, then chunks (ULONG length + data), then a zero-length terminator.
        buffer.writeInteger(UInt64(value.count), endianness: .little)
        var offset = 0
        let chunk = 0x7FFF_FFFF
        while offset < value.count {
            let length = min(chunk, value.count - offset)
            buffer.writeInteger(UInt32(length), endianness: .little)
            buffer.writeBytes(value[offset..<(offset + length)])
            offset += length
        }
        buffer.writeInteger(UInt32(0), endianness: .little)
    }
}
