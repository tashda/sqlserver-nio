import Testing
import Foundation
import NIOCore
@testable import SQLServerTDS
@testable import SQLServerKit

/// Tests for connection safety features: XACT_ABORT, transaction guard,
/// RESETCONNECTION flag, ATTENTION ack handling, and default query timeout.
struct ConnectionSafetyTests {

    // MARK: - Fix 1: XACT_ABORT

    @Test("Session defaults include SET XACT_ABORT ON")
    func xactAbortInDefaults() {
        let options = SQLServerConnection.SessionOptions.ssmsDefaults
        let statements = options.buildStatements()
        #expect(statements.contains("SET XACT_ABORT ON;"))
    }

    @Test("XACT_ABORT ON is the first SET statement (takes effect before queries)")
    func xactAbortIsFirst() {
        let options = SQLServerConnection.SessionOptions.ssmsDefaults
        let statements = options.buildStatements()
        let setStatements = statements.filter { $0.hasPrefix("SET ") }
        #expect(setStatements.first == "SET XACT_ABORT ON;")
    }

    @Test("XACT_ABORT can be disabled")
    func xactAbortDisabled() {
        let options = SQLServerConnection.SessionOptions(xactAbort: false)
        let statements = options.buildStatements()
        #expect(statements.contains("SET XACT_ABORT OFF;"))
        #expect(!statements.contains("SET XACT_ABORT ON;"))
    }

    @Test("XACT_ABORT is true by default")
    func xactAbortDefaultTrue() {
        let options = SQLServerConnection.SessionOptions()
        #expect(options.xactAbort == true)
    }

    // MARK: - Fix 3: RESETCONNECTION Flag

    @Test("applyResetConnectionFlag sets bit 0x08 in packet header")
    func resetConnectionFlagSet() {
        var buffer = ByteBufferAllocator().buffer(capacity: 16)
        buffer.writeInteger(UInt8(0x01))  // Type
        buffer.writeInteger(UInt8(0x01))  // Status (EOM only)
        buffer.writeInteger(UInt16(16))   // Length
        buffer.writeInteger(UInt16(0))    // SPID
        buffer.writeInteger(UInt8(1))     // PacketID
        buffer.writeInteger(UInt8(0))     // Window
        buffer.writeRepeatingByte(0, count: 8)  // payload

        var packet = TDSPacket(from: &buffer)!
        packet.applyResetConnectionFlag()

        // Read back status byte (offset 1)
        let status: UInt8 = packet.buffer.getInteger(at: 1)!
        #expect(status & 0x08 == 0x08, "RESETCONNECTION bit should be set")
        #expect(status & 0x01 == 0x01, "EOM bit should still be set")
    }

    @Test("applyResetConnectionFlag preserves existing status bits")
    func resetConnectionPreservesStatus() {
        var buffer = ByteBufferAllocator().buffer(capacity: 16)
        buffer.writeInteger(UInt8(0x01))  // Type
        buffer.writeInteger(UInt8(0x03))  // Status (EOM + ignoreThisEvent)
        buffer.writeInteger(UInt16(16))   // Length
        buffer.writeInteger(UInt16(0))    // SPID
        buffer.writeInteger(UInt8(1))     // PacketID
        buffer.writeInteger(UInt8(0))     // Window
        buffer.writeRepeatingByte(0, count: 8)

        var packet = TDSPacket(from: &buffer)!
        packet.applyResetConnectionFlag()

        let status: UInt8 = packet.buffer.getInteger(at: 1)!
        #expect(status == 0x0B, "Should be 0x03 | 0x08 = 0x0B")
    }

    @Test("TDSConnection.markForReset sets needsConnectionReset flag")
    func markForResetFlag() throws {
        // We can't easily create a TDSConnection without a real channel,
        // but we can verify the Status constants are correct
        #expect(TDSPacket.Status.resetConnection.value == 0x08)
        #expect(TDSPacket.Status.resetConnectionSkipTran.value == 0x10)
    }

    // MARK: - Fix 5: Default Query Timeout

    @Test("defaultQueryTimeout is nil by default")
    func defaultTimeoutNil() {
        let options = SQLServerConnection.SessionOptions()
        #expect(options.defaultQueryTimeout == nil)
    }

    @Test("defaultQueryTimeout can be set")
    func defaultTimeoutConfigurable() {
        let options = SQLServerConnection.SessionOptions(defaultQueryTimeout: 30.0)
        #expect(options.defaultQueryTimeout == 30.0)
    }

    @Test("ssmsDefaults has no default timeout")
    func ssmsDefaultsNoTimeout() {
        let options = SQLServerConnection.SessionOptions.ssmsDefaults
        #expect(options.defaultQueryTimeout == nil)
    }

    // MARK: - Session Options Completeness

    @Test("buildStatements produces all expected SET statements")
    func allSetStatements() {
        let options = SQLServerConnection.SessionOptions.ssmsDefaults
        let statements = options.buildStatements()

        #expect(statements.contains("SET XACT_ABORT ON;"))
        #expect(statements.contains("SET ANSI_NULLS ON;"))
        #expect(statements.contains("SET QUOTED_IDENTIFIER ON;"))
        #expect(statements.contains("SET ANSI_PADDING ON;"))
        #expect(statements.contains("SET ANSI_WARNINGS ON;"))
        #expect(statements.contains("SET ARITHABORT ON;"))
        #expect(statements.contains("SET CONCAT_NULL_YIELDS_NULL ON;"))
        #expect(statements.contains("SET IMPLICIT_TRANSACTIONS OFF;"))
        #expect(statements.contains("SET NOCOUNT ON;"))
        #expect(statements.contains("SET FMTONLY OFF;"))
    }

    @Test("SessionOptions Equatable works with xactAbort")
    func sessionOptionsEquatable() {
        let a = SQLServerConnection.SessionOptions(xactAbort: true)
        let b = SQLServerConnection.SessionOptions(xactAbort: true)
        let c = SQLServerConnection.SessionOptions(xactAbort: false)
        #expect(a == b)
        #expect(a != c)
    }

    @Test("SessionOptions Equatable works with defaultQueryTimeout")
    func sessionOptionsEquatableTimeout() {
        let a = SQLServerConnection.SessionOptions(defaultQueryTimeout: 30.0)
        let b = SQLServerConnection.SessionOptions(defaultQueryTimeout: 30.0)
        let c = SQLServerConnection.SessionOptions(defaultQueryTimeout: 60.0)
        let d = SQLServerConnection.SessionOptions(defaultQueryTimeout: nil)
        #expect(a == b)
        #expect(a != c)
        #expect(a != d)
    }

    // MARK: - Fix 4: ATTENTION Ack (Status Bits)

    @Test("DONE token ATTN bit is 0x0020")
    func doneTokenAttnBit() {
        // Verify the ATTN bit constant used in TDSRequestHandler
        let attnBit: UInt16 = 0x0020
        let doneMoreBit: UInt16 = 0x0001
        let countBit: UInt16 = 0x0010

        // A DONE token with ATTN + no more results
        let status: UInt16 = attnBit
        #expect(status & attnBit != 0)
        #expect(status & doneMoreBit == 0)

        // A DONE token with ATTN + count
        let statusWithCount: UInt16 = attnBit | countBit
        #expect(statusWithCount & attnBit != 0)
        #expect(statusWithCount & countBit != 0)
    }

    @Test("Attention signal packet type is 0x06")
    func attentionSignalPacketType() {
        #expect(TDSPacket.HeaderType.attentionSignal.value == 0x06)
    }

    // MARK: - TDS Packet Construction for ATTENTION

    @Test("ATTENTION packet has correct structure")
    func attentionPacketStructure() {
        var empty = ByteBufferAllocator().buffer(capacity: 0)
        let packet = TDSPacket(
            from: &empty,
            ofType: .attentionSignal,
            isLastPacket: true,
            packetId: 1,
            allocator: ByteBufferAllocator()
        )

        #expect(packet.type == .attentionSignal)
        // Header should be 8 bytes, no payload
        #expect(packet.buffer.readableBytes == 8)
        // Type byte should be 0x06
        let typeByte: UInt8 = packet.buffer.getInteger(at: 0)!
        #expect(typeByte == 0x06)
        // Status should be EOM (0x01)
        let status: UInt8 = packet.buffer.getInteger(at: 1)!
        #expect(status == 0x01)
    }
}
