import NIO
import Foundation

extension TDSMessages {
    /// `PRELOGIN`
    /// https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/60f56408-0188-4cd5-8b90-25c6f2423868
    public struct PreloginResponse: TDSMessagePayload {
        public static var packetType: TDSPacket.HeaderType {
            return .prelogin
        }

        public let version: String
        public let encryption: PreloginEncryption

        public init(version: String, encryption: PreloginEncryption) {
            self.version = version
            self.encryption = encryption
        }

        public static func parse(from buffer: inout ByteBuffer) throws -> PreloginResponse {
            var input = buffer
            let messageStart = input.readerIndex
            var options: [(token: UInt8, offset: Int, length: Int)] = []

            while true {
                guard let typeByte = input.readByte() else { throw TDSError.needMoreData }
                if typeByte == 0xFF { break }
                guard let offset: UInt16 = input.readInteger(endianness: .big),
                      let length: UInt16 = input.readInteger(endianness: .big) else {
                    throw TDSError.needMoreData
                }
                options.append((typeByte, Int(offset), Int(length)))
            }

            let optionTableEnd = input.readerIndex
            var version: String?
            var encryption: PreloginEncryption?
            for option in options {
                let dataStart = messageStart + option.offset
                guard dataStart >= optionTableEnd else {
                    throw TDSError.protocolError("PRELOGIN response data overlaps option table")
                }
                guard let data = input.getSlice(at: dataStart, length: option.length) else {
                    throw TDSError.needMoreData
                }
                var value = data
                switch option.token {
                case 0x00:
                    guard option.length == 6,
                          let major: UInt8 = value.readInteger(),
                          let minor: UInt8 = value.readInteger(),
                          let build: UInt16 = value.readInteger(endianness: .big) else {
                        throw TDSError.protocolError("Invalid PRELOGIN VERSION data")
                    }
                    version = "\(major).\(minor).\(build)"
                case 0x01:
                    guard option.length == 1,
                          let encryptionByte = value.readByte(),
                          let parsed = PreloginEncryption(rawValue: encryptionByte) else {
                        throw TDSError.protocolError("Invalid PRELOGIN ENCRYPTION data")
                    }
                    encryption = parsed
                default:
                    break
                }
            }

            guard let version = version else {
                throw TDSError.protocolError("Invalid PRELOGIN response: missing VERSION")
            }
            guard let encryption = encryption else {
                throw TDSError.protocolError("Invalid PRELOGIN response: missing ENCRYPTION")
            }
            return PreloginResponse(version: version, encryption: encryption)
        }
    }
}
