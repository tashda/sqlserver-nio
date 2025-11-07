extension TDSTokenParser {
    public static func parseLoginAckToken(from buffer: inout ByteBuffer) throws -> TDSTokens.LoginAckToken {
        guard buffer.readUShort() != nil else {
            throw TDSError.needMoreData
        }
        guard let interface = buffer.readByte() else {
            throw TDSError.needMoreData
        }
        guard let tdsVersion = buffer.readDWord() else {
            throw TDSError.needMoreData
        }
        guard let progNameLength = buffer.readByte() else {
            throw TDSError.needMoreData
        }
        guard let progName = buffer.readUTF16String(length: Int(progNameLength) * 2) else {
            throw TDSError.needMoreData
        }
        guard let majorVer = buffer.readByte(),
              let minorVer = buffer.readByte(),
              let buildNumHi = buffer.readByte(),
              let buildNumLow = buffer.readByte()
        else {
            throw TDSError.needMoreData
        }

        let token = TDSTokens.LoginAckToken(interface: interface, tdsVersion: tdsVersion, progName: progName, majorVer: majorVer, minorVer: minorVer, buildNumHi: buildNumHi, buildNumLow: buildNumLow)

        return token
    }
}
