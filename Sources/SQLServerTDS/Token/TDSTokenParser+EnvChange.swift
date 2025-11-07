extension TDSTokenParser {
    public static func parseEnvChangeToken(from buffer: inout ByteBuffer) throws -> TDSToken {
        guard buffer.readUShort() != nil else {
            throw TDSError.needMoreData
        }
        guard let type = buffer.readByte() else {
            throw TDSError.needMoreData
        }
        guard let changeType = TDSTokens.EnvchangeType(rawValue: type) else {
            throw TDSError.protocolError("Invalid envchange token")
        }

        switch changeType {
        case .database, .language, .characterSet, .packetSize, .realTimeLogShipping, .unicodeSortingLocalId, .unicodeSortingFlags, .userInstanceStarted:
            guard
                let newValue = buffer.readBVarchar(),
                let oldValue = buffer.readBVarchar()
            else {
                throw TDSError.needMoreData
            }

            let token = TDSTokens.EnvchangeToken<String>(envchangeType: changeType, newValue: newValue, oldValue: oldValue)
            return token

        case .sqlCollation, .beingTransaction, .commitTransaction, .defectTransaction, .rollbackTransaction, .enlistDTCTransaction, .resetConnectionAck, .transactionEnded:
            guard
                let newValue = buffer.readBVarbyte(),
                let oldValue = buffer.readBVarbyte()
            else {
                throw TDSError.needMoreData
            }

            let token = TDSTokens.EnvchangeToken<[Byte]>(envchangeType: changeType, newValue: newValue, oldValue: oldValue)
            return token

        case .promoteTransaction:
            guard
                let newValue = buffer.readLVarbyte(),
                let _ = buffer.readBytes(length: 1)
            else {
                throw TDSError.needMoreData
            }

            let token = TDSTokens.EnvchangeToken<[Byte]>(envchangeType: changeType, newValue: newValue, oldValue: [])
            return token

        case .transactionManagerAddress:
            throw TDSError.protocolError("Received unexpected ENVCHANGE Token Type 16: Transaction Manager Address is not used by SQL Server.")

        case .routingInfo:
            guard buffer.readUShort() != nil else {
                throw TDSError.needMoreData
            }
            guard let protocolByte = buffer.readByte() else {
                throw TDSError.needMoreData
            }
            guard protocolByte == 0 else {
                throw TDSError.protocolError("Invalid routing protocol \(protocolByte)")
            }
            guard let portNumber = buffer.readUShort() else {
                throw TDSError.needMoreData
            }
            guard let alternateServer = buffer.readUSVarchar() else {
                throw TDSError.needMoreData
            }
            guard let oldValue = buffer.readBytes(length: 2) else {
                throw TDSError.needMoreData
            }

            let newValue = TDSTokens.RoutingEnvchangeToken.RoutingData(port: Int(portNumber), alternateServer: alternateServer)

            let token = TDSTokens.RoutingEnvchangeToken(envchangeType: changeType, newValue: newValue, oldValue: oldValue)
            return token
        }
    }
}
