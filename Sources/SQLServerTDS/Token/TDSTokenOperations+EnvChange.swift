import NIOCore

extension TDSTokenOperations {
    internal static func parseEnvChangeToken(from buffer: inout ByteBuffer) throws -> TDSToken {
        guard let length: UInt16 = buffer.readInteger(endianness: .little) else {
            throw TDSError.needMoreData
        }
        let payloadEnd = buffer.readerIndex + Int(length)

        guard let type: UInt8 = buffer.readInteger() else {
            throw TDSError.needMoreData
        }

        let changeType = TDSTokens.EnvChangeType(rawValue: type)
        let token: TDSToken

        switch changeType {
        case .database, .language, .charset, .packetSize, .userInstance:
            guard
                let newLength: UInt8 = buffer.readInteger(),
                let newValue = buffer.readUTF16String(length: Int(newLength) * 2),
                let oldLength: UInt8 = buffer.readInteger(),
                let oldValue = buffer.readUTF16String(length: Int(oldLength) * 2)
            else {
                throw TDSError.needMoreData
            }
            token = TDSTokens.EnvchangeToken<String>(envType: type, newValue: newValue, oldValue: oldValue)

        case .sqlCollation, .beginTransaction, .commitTransaction, .rollbackTransaction,
             .enlistDTCTransaction, .defectTransaction, .transactionEnded, .resetConnectionAck:
            guard
                let newLength: UInt8 = buffer.readInteger(),
                let newValue = buffer.readBytes(length: Int(newLength)),
                let oldLength: UInt8 = buffer.readInteger(),
                let oldValue = buffer.readBytes(length: Int(oldLength))
            else {
                throw TDSError.needMoreData
            }
            token = TDSTokens.EnvchangeToken<[UInt8]>(envType: type, newValue: newValue, oldValue: oldValue)

        default:
            let remaining = max(0, payloadEnd - buffer.readerIndex)
            guard let payload = buffer.readBytes(length: remaining) else {
                throw TDSError.needMoreData
            }
            token = TDSTokens.EnvchangeToken<[UInt8]>(envType: type, newValue: payload, oldValue: [])
        }

        buffer.moveReaderIndex(to: payloadEnd)

        return token
    }
}
