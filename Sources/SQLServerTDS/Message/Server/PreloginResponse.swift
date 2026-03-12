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
            var _buffer = buffer
            
            var version: String?
            var encryption: PreloginEncryption?
            
            while true {
                guard let typeByte = _buffer.readByte() else {
                    throw TDSError.needMoreData
                }
                
                guard let token = PreloginToken(rawValue: typeByte) else {
                    throw TDSError.protocolError("Invalid Prelogin Response: Unknown token 0x\(String(format: "%02X", typeByte))")
                }
                
                if token == .terminator {
                    break
                }
                
                // Read PRELOGIN_OPTION
                guard
                    let offset: UInt16 = _buffer.readInteger(endianness: .big),
                    let _: UInt16 = _buffer.readInteger(endianness: .big)
                    else {
                        throw TDSError.protocolError("Invalid Prelogin Response: Invalid *PRELOGIN_OPTION segment.")
                }
                
                let savedIndex = _buffer.readerIndex
                _buffer.moveReaderIndex(to: Int(offset))
                
                switch token {
                case .version:
                    guard
                        let major: UInt8 = _buffer.readInteger(),
                        let minor: UInt8 = _buffer.readInteger(),
                        let build: UInt16 = _buffer.readInteger(endianness: .big)
                        else {
                            throw TDSError.protocolError("Invalid Prelogin Response: Invalid VERSION data.")
                    }
                    version = "\(major).\(minor).\(build)"
                case .encryption:
                    guard let encryptionByte = _buffer.readByte() else {
                        throw TDSError.protocolError("Invalid Prelogin Response: Invalid ENCRYPTION data.")
                    }
                    encryption = PreloginEncryption(rawValue: encryptionByte)
                default:
                    break
                }
                
                _buffer.moveReaderIndex(to: savedIndex)
            }
            
            guard let version = version else {
                throw TDSError.protocolError("Invalid Prelogin Response: Missing required VERSION data.")
            }
            
            guard let encryption = encryption else {
                throw TDSError.protocolError("Invalid Prelogin Response: Missing required ENCRYPTION data.")
            }
            
            let response = PreloginResponse(version: version, encryption: encryption)
            return response
        }
    }
}
