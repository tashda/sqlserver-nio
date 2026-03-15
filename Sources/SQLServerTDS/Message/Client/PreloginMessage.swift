import NIO

extension TDSMessages {
    /// `PRELOGIN`
    /// https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/60f56408-0188-4cd5-8b90-25c6f2423868
    public struct PreloginMessage: TDSMessagePayload {
        public static var packetType: TDSPacket.HeaderType {
            return .prelogin
        }
        
        public static let messageLength: Byte = 0x1A // (26 bytes)
        
        public var version: String
        public var encryption: PreloginEncryption?
        public var fedAuthRequired: Bool

        public init(version: String, encryption: PreloginEncryption?, fedAuthRequired: Bool = false) {
            self.version = version
            self.encryption = encryption
            self.fedAuthRequired = fedAuthRequired
        }
        
        public func serialize(into buffer: inout ByteBuffer) throws {
            // Build option table dynamically to handle variable offsets.
            // Each option entry: Token(1) + Offset(2) + Length(2) = 5 bytes
            // Plus terminator: 1 byte

            // Calculate how many option entries we have
            var optionCount = 1 // VERSION is always present
            if encryption != nil { optionCount += 1 }
            if fedAuthRequired { optionCount += 1 } // FEDAUTHREQUIRED
            let optionTableSize = optionCount * 5 + 1 // +1 for terminator

            // Data sizes
            let versionDataSize = 6
            let encryptionDataSize = encryption != nil ? 1 : 0
            let fedAuthDataSize = fedAuthRequired ? 1 : 0

            // Calculate data offsets (from start of message)
            var currentOffset = optionTableSize
            let versionOffset = currentOffset
            currentOffset += versionDataSize
            let encryptionOffset = currentOffset
            currentOffset += encryptionDataSize
            let fedAuthOffset = currentOffset

            // Write option table
            // VERSION
            buffer.writeInteger(UInt8(0x00)) // token
            buffer.writeInteger(UInt16(versionOffset), endianness: .big)
            buffer.writeInteger(UInt16(versionDataSize), endianness: .big)

            // ENCRYPTION
            if encryption != nil {
                buffer.writeInteger(UInt8(0x01))
                buffer.writeInteger(UInt16(encryptionOffset), endianness: .big)
                buffer.writeInteger(UInt16(encryptionDataSize), endianness: .big)
            }

            // FEDAUTHREQUIRED
            if fedAuthRequired {
                buffer.writeInteger(UInt8(0x06))
                buffer.writeInteger(UInt16(fedAuthOffset), endianness: .big)
                buffer.writeInteger(UInt16(fedAuthDataSize), endianness: .big)
            }

            // Terminator
            buffer.writeInteger(UInt8(0xFF))

            // Write data section
            // Version Data
            buffer.writeBytes([
                0x09, 0x00, 0x00, 0x00,     // UL_VERSION (9.0.0)
                0x00, 0x00,                 // US_SUBBUILD (0)
            ])

            // Encryption Data
            if let enc = encryption {
                buffer.writeInteger(enc.rawValue)
            }

            // FEDAUTHREQUIRED Data
            if fedAuthRequired {
                buffer.writeInteger(UInt8(0x01)) // federated auth is required
            }
        }
    }
}

public struct PreloginOption {
    /// `PL_OPTION_TOKEN`
    var token: TDSMessages.PreloginToken
    /// `PL_OFFSET`
    var offset: UShort
    /// `PL_OPTION_LENGTH`
    var length: UShort
}

extension TDSMessages {
    public enum PreloginToken: Byte {
        /// VERSION
        case version = 0x00
        
        /// ENCRYPTION
        case encryption = 0x01
        
        /// INSTOPT
        case instOpt = 0x02
        
        /// THREADID
        case threadId = 0x03
        
        /// MARS
        case mars = 0x04
        
        /// TRACEID
        case traceId = 0x05
        
        // FEDAUTHREQUIRED
        case fedAuthRequired = 0x06
        
        // NONCEOPT
        case nonceOpt = 0x07
        
        // TERMINATOR
        case terminator = 0xFF
    }
}

/// High-level encryption mode for TDS connections.
/// Maps to the ENCRYPT connection string option.
public enum TDSEncryptionMode: Sendable {
    /// Encryption is optional. Use TLS if available, fall back to unencrypted.
    case optional
    /// Encryption is mandatory. Fail if server doesn't support TLS.
    case mandatory
    /// TDS 8.0 strict mode. TLS before any TDS traffic.
    case strict
}

extension TDSMessages {
    public enum PreloginEncryption: Byte, Sendable {
        case encryptOff = 0x00
        case encryptOn = 0x01
        case encryptNotSup = 0x02
        case encryptReq = 0x03
        case encryptClientCertOff = 0x80
        case encryptClientCertOn = 0x81
        case encryptClientCertReq = 0x83
    }
}
