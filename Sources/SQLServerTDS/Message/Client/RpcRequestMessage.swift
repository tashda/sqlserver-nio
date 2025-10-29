import NIO
import Foundation

extension TDSMessages {
    public struct RpcParameter {
        public enum Direction {
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

    public struct RpcRequestMessage: TDSMessagePayload {
        public static let packetType: TDSPacket.HeaderType = .rpc

        public let procedureName: String
        public let parameters: [RpcParameter]
        public let transactionDescriptor: [UInt8]
        public let outstandingRequestCount: UInt32

        public init(procedureName: String, parameters: [RpcParameter], transactionDescriptor: [UInt8] = [0,0,0,0,0,0,0,0], outstandingRequestCount: UInt32 = 1) {
            self.procedureName = procedureName
            self.parameters = parameters
            self.transactionDescriptor = transactionDescriptor
            self.outstandingRequestCount = outstandingRequestCount
        }

        public func serialize(into buffer: inout ByteBuffer) throws {
            // Write ALL_HEADERS (MARS/Transaction) like Microsoft JDBC
            TDSMessage.serializeAllHeaders(&buffer, transactionDescriptor: transactionDescriptor, outstandingRequestCount: outstandingRequestCount)
            // RPC target selector and procedure name encoding
            // Per MS-TDS, for RPCRequest the procedure identifier is either:
            //  - US_VARCHAR name: 2-byte byte length followed by UTF-16LE characters
            //  - or a ProcID indicated by a length of 0xFFFF, followed by a 2-byte ProcID.
            // We are calling user procedures by name, so encode US_VARCHAR and DO NOT prefix 0xFFFF.
            // Option flags (2 bytes) immediately follow the name/ProcID.
            let procChars = Array(procedureName.utf16)
            let procMode = ProcessInfo.processInfo.environment["TDS_RPC_PROCNAME_MODE"].flatMap { Int($0) } ?? 1
            switch procMode {
            case 2:
                // 0xFFFF selector + B_VARCHAR (1-byte char count + UTF-16LE)
                buffer.writeInteger(UInt16(0xFFFF), endianness: .little)
                guard procChars.count <= 0xFF else { throw TDSError.protocolError("RPC procedure name too long") }
                buffer.writeInteger(UInt8(procChars.count))
                for ch in procChars { buffer.writeInteger(ch, endianness: .little) }
            case 3:
                // 0xFFFF selector + US_VARCHAR (2-byte byte length + UTF-16LE)
                buffer.writeInteger(UInt16(0xFFFF), endianness: .little)
                fallthrough
            default:
                // US_VARCHAR name
                let byteLen = procChars.count &* 2
                guard byteLen <= 0xFFFF else { throw TDSError.protocolError("RPC procedure name too long") }
                buffer.writeInteger(UInt16(byteLen), endianness: .little)
                for ch in procChars { buffer.writeInteger(ch, endianness: .little) }
            }
            // RPC option flags (2 bytes): both zero for default behavior
            buffer.writeInteger(UInt16(0), endianness: .little)

            // Parameters
            let useParamNameASCII = ProcessInfo.processInfo.environment["TDS_RPC_PARAMNAME_ASCII"] == "1"
            let includeDecScaleInTypeInfo = ProcessInfo.processInfo.environment["TDS_RPC_DEC_TYPEINFO_SCALE"] == "1"
            let outIntLenZero = ProcessInfo.processInfo.environment["TDS_RPC_OUT_INT_LEN0"] == "1"
            for p in parameters {
                // Param name: by default use UTF-16LE with 1-byte character count (matches JDBC);
                // allow ASCII B_VARCHAR via env for troubleshooting with some servers / dissectors.
                let rawName = p.name.hasPrefix("@") ? String(p.name.dropFirst()) : p.name
                let paramName = "@" + rawName
                if useParamNameASCII, let ascii = paramName.data(using: .ascii) {
                    guard ascii.count <= 0xFF else { throw TDSError.protocolError("RPC parameter name too long: \(p.name)") }
                    buffer.writeInteger(UInt8(ascii.count))
                    buffer.writeBytes(ascii)
                } else {
                    let nameChars = Array(paramName.utf16)
                    guard nameChars.count <= 0xFF else { throw TDSError.protocolError("RPC parameter name too long: \(p.name)") }
                    buffer.writeInteger(UInt8(nameChars.count))
                    for ch in nameChars { buffer.writeInteger(ch, endianness: .little) }
                }

                // Status: 1 byte; 0x01 for by-ref (output); 0x00 for input
                let byRef: UInt8 = (p.direction == .out || p.direction == .inout) ? 0x01 : 0x00
                buffer.writeInteger(byRef)

                // TYPE_INFO and value
                try writeTypeInfoAndValue(param: p, into: &buffer, includeDecScaleInTypeInfo: includeDecScaleInTypeInfo, outIntLenZero: outIntLenZero)
            }
        }

        private func writeTypeInfoAndValue(param: RpcParameter, into buffer: inout ByteBuffer, includeDecScaleInTypeInfo: Bool, outIntLenZero: Bool) throws {
            // NULL handling: for variable types, send NULL marker in value length field; for INTN family send ByteLen=0
            func writeNull(for type: TDSDataType, into buf: inout ByteBuffer) {
                switch type {
                case .int, .smallInt, .tinyInt, .bigInt, .intn, .bit, .bitn:
                    // Write TYPE_INFO for INTN with length then ByteLen=0 for value
                    buf.writeInteger(TDSDataType.intn.rawValue)
                    buf.writeInteger(UInt8(0)) // ByteLen = 0 indicates NULL
                case .varchar, .char, .nchar, .nvarchar, .binary, .varbinary:
                    // TYPE_INFO w/ max length (use 0 for safety), then UShortCharBinLen=0xFFFF for NULL
                    buf.writeInteger(type.rawValue)
                    buf.writeInteger(UInt16(0), endianness: .little)
                    if type == .varchar || type == .char { /* collation */ buf.writeBytes([0,0,0,0,0]) }
                    buf.writeInteger(UInt16(0xFFFF), endianness: .little)
                default:
                    // Fallback to intn null
                    buf.writeInteger(TDSDataType.intn.rawValue)
                    buf.writeInteger(UInt8(0))
                }
            }

            guard let data = param.data else {
                writeNull(for: .intn, into: &buffer)
                return
            }

            switch data.metadata.dataType {
            case .decimal, .numeric, .decimalLegacy, .numericLegacy:
                // TYPE_INFO for RPC decimal/numeric follows Microsoft JDBC behavior:
                //  - type (DECIMALN/NUMERICN)
                //  - max length = 0x11 (17 bytes incl. sign)
                //  - precision = 38 (MAX)
                // The scale is typically carried in the VALUE bytes; however some servers expect scale in TYPE_INFO.
                // Enable writing scale into TYPE_INFO with TDS_RPC_DEC_TYPEINFO_SCALE=1.
                buffer.writeInteger(data.metadata.dataType.rawValue)
                let scale = UInt8(data.metadata.scale ?? 0)
                buffer.writeInteger(UInt8(0x11))
                buffer.writeInteger(UInt8(38))
                if includeDecScaleInTypeInfo {
                    buffer.writeInteger(scale)
                }
                // VALUE
                // For OUT parameters, send a NULL value (ByteLen=0) and let the server populate it.
                if param.direction != .in {
                    if !includeDecScaleInTypeInfo { buffer.writeInteger(scale) }
                    buffer.writeInteger(UInt8(0))
                } else if var val = data.value {
                    // Our TDSData decimal payload is [sign(1), magnitude(fixed width little-endian)].
                    guard let sign = val.readInteger(as: UInt8.self) else { if !includeDecScaleInTypeInfo { buffer.writeInteger(scale) }; buffer.writeInteger(UInt8(0)); return }
                    let fixedMag = val.readBytes(length: val.readableBytes) ?? []
                    // Trim high-order zero bytes to minimal length while preserving at least one byte
                    var magLen = fixedMag.count
                    while magLen > 1 && fixedMag[magLen - 1] == 0 { magLen -= 1 }
                    if !includeDecScaleInTypeInfo { buffer.writeInteger(scale) }
                    buffer.writeInteger(UInt8(magLen + 1)) // length includes sign
                    buffer.writeInteger(sign)
                    buffer.writeBytes(fixedMag.prefix(magLen))
                } else {
                    if !includeDecScaleInTypeInfo { buffer.writeInteger(scale) }
                    buffer.writeInteger(UInt8(0)) // NULL
                }
            case .int, .smallInt, .tinyInt, .bigInt, .intn:
                // TYPE_INFO: INTN -> type(1) + maxLen(1)
                buffer.writeInteger(TDSDataType.intn.rawValue)
                let maxLen: UInt8 = {
                    switch data.metadata.dataType {
                    case .tinyInt: return 1
                    case .smallInt: return 2
                    case .int: return 4
                    case .bigInt: return 8
                    case .intn:
                        return UInt8(min(max(data.value?.readableBytes ?? 4, 1), 8))
                    default: return 4
                    }
                }()
                buffer.writeInteger(maxLen)
                // Value: ByteLen (0,1,2,4,8) + bytes
                if param.direction != .in {
                    buffer.writeInteger(UInt8(0))
                } else {
                    guard var val = data.value else { buffer.writeInteger(UInt8(0)); return }
                    let len = UInt8(val.readableBytes)
                    buffer.writeInteger(len)
                    if len > 0, let bytes = val.readBytes(length: Int(len)) { buffer.writeBytes(bytes) }
                }
            case .bit, .bitn:
                // TYPE_INFO: BITN -> type(1) + length(1)
                buffer.writeInteger(TDSDataType.bitn.rawValue)
                buffer.writeInteger(UInt8(1))
                if let b = data.value?.getInteger(at: data.value!.readerIndex, as: UInt8.self) {
                    buffer.writeInteger(UInt8(1))
                    buffer.writeInteger(b)
                } else {
                    buffer.writeInteger(UInt8(0))
                }
            case .varchar, .char:
                // TYPE_INFO: type + max length + collation(5)
                buffer.writeInteger(data.metadata.dataType.rawValue)
                let payloadLen = UInt16(min(data.value?.readableBytes ?? 0, 8000))
                buffer.writeInteger(payloadLen, endianness: .little)
                buffer.writeBytes(collationBytes(from: data.metadata.collation))
                // Value: UShortCharBinLen (0xFFFF for NULL; here we send exact length)
                if let val = data.value, let bytes = val.getBytes(at: val.readerIndex, length: val.readableBytes) {
                    buffer.writeInteger(UInt16(bytes.count), endianness: .little)
                    buffer.writeBytes(bytes)
                } else {
                    buffer.writeInteger(UInt16(0), endianness: .little)
                }
            case .nvarchar, .nchar:
                buffer.writeInteger(data.metadata.dataType.rawValue)
                let payloadLen = UInt16(min(data.value?.readableBytes ?? 0, 4000 * 2))
                buffer.writeInteger(payloadLen, endianness: .little)
                buffer.writeBytes(collationBytes(from: data.metadata.collation))
                if let val = data.value, let bytes = val.getBytes(at: val.readerIndex, length: val.readableBytes) {
                    buffer.writeInteger(UInt16(bytes.count), endianness: .little)
                    buffer.writeBytes(bytes)
                } else {
                    buffer.writeInteger(UInt16(0), endianness: .little)
                }
            case .varbinary, .binary:
                buffer.writeInteger(data.metadata.dataType.rawValue)
                let payloadLen = UInt16(min(data.value?.readableBytes ?? 0, 8000))
                buffer.writeInteger(payloadLen, endianness: .little)
                if let val = data.value, let bytes = val.getBytes(at: val.readerIndex, length: val.readableBytes) {
                    buffer.writeInteger(UInt16(bytes.count), endianness: .little)
                    buffer.writeBytes(bytes)
                } else {
                    buffer.writeInteger(UInt16(0), endianness: .little)
                }
            case .guid:
                // TYPE_INFO: GUIDTYPE -> type(1) + length(1)
                buffer.writeInteger(TDSDataType.guid.rawValue)
                buffer.writeInteger(UInt8(16))
                // Value: fixed 16 bytes
                if let val = data.value, let bytes = val.getBytes(at: val.readerIndex, length: val.readableBytes), bytes.count == 16 {
                    buffer.writeBytes(bytes)
                } else {
                    buffer.writeBytes([UInt8](repeating: 0, count: 16))
                }
            default:
                throw TDSError.protocolError("Unsupported RPC param type: \(data.metadata.dataType)")
            }
        }

        private func collationBytes(from source: [UInt8]) -> [UInt8] {
            if source.count == 5 { return source }
            return [0,0,0,0,0]
        }
    }
}
