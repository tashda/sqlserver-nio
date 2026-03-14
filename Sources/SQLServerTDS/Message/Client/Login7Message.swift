import Logging
import NIO
import Foundation

extension TDSMessages {
    /// `LOGIN7`
    /// https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/773a62b6-ee89-4c02-9e5e-344882630aac
    public struct Login7Message: TDSMessagePayload, Sendable {
        public static var packetType: TDSPacket.HeaderType {
            return .tds7Login
        }

        static let clientPID = UInt32(ProcessInfo.processInfo.processIdentifier)

        var username: String
        var password: String
        var serverName: String
        var database: String
        var useIntegratedSecurity: Bool = false
        var sspiData: Data?
        /// Pre-acquired access token for Entra ID / Azure AD federated authentication.
        var fedAuthAccessToken: String?

        public func serialize(into buffer: inout ByteBuffer) throws {
            let passwordField = useIntegratedSecurity ? "" : password
            let optionFlags2: UInt8 = useIntegratedSecurity ? 0x83 : 0x03
            let hasFedAuth = fedAuthAccessToken != nil
            let optionFlags3: UInt8 = hasFedAuth ? 0x10 : 0x00 // fExtension = bit 4

            // Each basic field needs to serialize the length & offset
            let basicFields = [
                (Host.current().name ?? "", false),
                (username, false),
                (passwordField, true),
                ("", false),
                (serverName, false),
                ("", false), // extension field (patched below for fedAuth)
                ("swift-tds", false),
                ("", false),
                (database, false)
            ]

            // ClientID serializes inbetween `basicFields` and `extendedFields`
            let clientId: [UInt8] = [0x00, 0x50, 0x8b, 0xe3, 0xb7, 0x8f]

            // Each extended field needs to serialize the length & offset
            let extendedFields = [
                ("", false),
                ("", false),
                ("", true)
            ]

            // Stores the position and skips an UInt32 so the length can be added later
            let login7HeaderPosition = buffer.writerIndex
            buffer.moveWriterIndex(forwardBy: 4)

            buffer.writeBytes([
                0x04, 0x00, 0x00, 0x74, // TDS version 7.4 (SQL Server 2012+, required for SQL Server 2025)
                0x00, 0x10, 0x00, 0x00, // Packet length negotiation
                0x00, 0x00, 0x00, 0x01, // Client version, 0x07 in example
            ])

            buffer.writeInteger(Self.clientPID)
            buffer.writeInteger(0 as UInt32) // Connection ID
            buffer.writeInteger(0xE0 as UInt8) // Flags1
            buffer.writeInteger(optionFlags2) // Flags2
            buffer.writeInteger(0 as UInt8) // TypeFlags
            buffer.writeInteger(optionFlags3) // Flags3
            buffer.writeInteger(0 as UInt32) // Timezone
            buffer.writeBytes([0x09, 0x04, 0x00, 0x00]) // ClientLCID

            // Save position of the ibExtension field (6th offset/length pair, index 5)
            // Each pair is 4 bytes (offset: UInt16 + length: UInt16)
            let extensionOffsetPosition = buffer.writerIndex + 5 * 4 // 5th entry (0-indexed)

            var offsetLengthsPosition = buffer.writerIndex
            buffer.moveWriterIndex(forwardBy: basicFields.count * 4)
            buffer.writeBytes(clientId)

            buffer.moveWriterIndex(forwardBy: extendedFields.count * 4)

            let sspiOffsetPosition = buffer.writerIndex
            buffer.moveWriterIndex(forwardBy: 4)

            func writeField(_ string: String, isPassword: Bool) {
                let utf16 = string.utf16

                buffer.setInteger(UInt16(buffer.writerIndex - login7HeaderPosition), at: offsetLengthsPosition, endianness: .little)
                offsetLengthsPosition += 2
                buffer.setInteger(UInt16(utf16.count), at: offsetLengthsPosition, endianness: .little)
                offsetLengthsPosition += 2

                if isPassword {
                    for character in utf16 {
                        let newHighBits = (character << 4) & 0b1111000011110000
                        let newLowBits = (character >> 4) & 0b0000111100001111
                        buffer.writeInteger((newHighBits | newLowBits) ^ 0xA5A5, endianness: .little)
                    }
                } else {
                    for character in utf16 {
                        buffer.writeInteger(character, endianness: .little)
                    }
                }
            }

            for (field, isPassword) in basicFields {
                writeField(field, isPassword: isPassword)
            }

            offsetLengthsPosition += clientId.count

            for (field, isPassword) in extendedFields {
                writeField(field, isPassword: isPassword)
            }

            if let sspiData, !sspiData.isEmpty {
                let offset = UInt16(buffer.writerIndex - login7HeaderPosition)
                buffer.setInteger(offset, at: sspiOffsetPosition, endianness: .little)
                buffer.setInteger(UInt16(sspiData.count), at: sspiOffsetPosition + 2, endianness: .little)
                buffer.writeBytes(sspiData)
            } else {
                buffer.setInteger(UInt16(0), at: sspiOffsetPosition, endianness: .little)
                buffer.setInteger(UInt16(0), at: sspiOffsetPosition + 2, endianness: .little)
            }

            // FEDAUTH FeatureExt — appended after all other variable data
            if hasFedAuth, let tokenString = fedAuthAccessToken {
                // The extension field (ibExtension/cbExtension) points to a 4-byte DWORD
                // that contains the offset to the actual FeatureExt block.
                let extensionPointerOffset = UInt16(buffer.writerIndex - login7HeaderPosition)
                buffer.setInteger(extensionPointerOffset, at: extensionOffsetPosition, endianness: .little)
                buffer.setInteger(UInt16(4), at: extensionOffsetPosition + 2, endianness: .little)

                // Write the 4-byte offset pointer to FeatureExt block
                // The FeatureExt starts right after this DWORD
                let featureExtOffset = UInt32(buffer.writerIndex - login7HeaderPosition + 4)
                buffer.writeInteger(featureExtOffset, endianness: .little)

                // Write FeatureExt block
                // FEDAUTH feature (ID 0x02)
                let tokenBytes = Array(tokenString.utf8)
                // bOptions: (SECURITY_TOKEN << 1) | fFedAuthEcho
                // SECURITY_TOKEN = 0x02, no echo = 0
                let bOptions: UInt8 = 0x02 << 1 // = 0x04
                let featureDataLen = UInt32(1 + 4 + tokenBytes.count) // bOptions + tokenLen + token

                buffer.writeInteger(UInt8(0x02)) // FeatureId = FEDAUTH
                buffer.writeInteger(featureDataLen, endianness: .little)
                buffer.writeInteger(bOptions)
                buffer.writeInteger(UInt32(tokenBytes.count), endianness: .little)
                buffer.writeBytes(tokenBytes)

                // FeatureExt terminator
                buffer.writeInteger(UInt8(0xFF))
            }

            buffer.setInteger(UInt32(buffer.writerIndex - login7HeaderPosition), at: login7HeaderPosition, endianness: .little)
            return
        }
    }
}
