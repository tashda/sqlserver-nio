import Foundation
import Testing
@testable import SQLServerTDS

@Suite("TDS Collation Code Page Mapping")
struct TDSCollationTests {

    // MARK: - Empty / Invalid Collation

    @Test("Empty collation falls back to CP1252")
    func emptyCollation() {
        let encoding = TDSCollation.encoding(from: [])
        #expect(encoding == .windowsCP1252)
    }

    @Test("Short collation falls back to CP1252")
    func shortCollation() {
        let encoding = TDSCollation.encoding(from: [0x09, 0x04])
        #expect(encoding == .windowsCP1252)
    }

    // MARK: - Windows Collations (SortId == 0)

    @Test("Danish LCID 0x0406 maps to CP1252")
    func danishLCID() {
        // LCID 1030 = 0x0406 → Danish
        // Bytes: [0x06, 0x04, 0x00, 0x00, 0x00] (SortId = 0)
        let collation: [UInt8] = [0x06, 0x04, 0x00, 0x00, 0x00]
        let codePage = TDSCollation.codePage(from: collation)
        #expect(codePage == 1252)
    }

    @Test("US English LCID 0x0409 maps to CP1252")
    func englishLCID() {
        let collation: [UInt8] = [0x09, 0x04, 0x00, 0x00, 0x00]
        let codePage = TDSCollation.codePage(from: collation)
        #expect(codePage == 1252)
    }

    @Test("Russian LCID 0x0419 maps to CP1251")
    func russianLCID() {
        // LCID 1049 = 0x0419
        let collation: [UInt8] = [0x19, 0x04, 0x00, 0x00, 0x00]
        let codePage = TDSCollation.codePage(from: collation)
        #expect(codePage == 1251)
    }

    @Test("Japanese LCID 0x0411 maps to CP932")
    func japaneseLCID() {
        let collation: [UInt8] = [0x11, 0x04, 0x00, 0x00, 0x00]
        let codePage = TDSCollation.codePage(from: collation)
        #expect(codePage == 932)
    }

    @Test("Arabic LCID maps to CP1256")
    func arabicLCID() {
        // LCID 1025 = 0x0401
        let collation: [UInt8] = [0x01, 0x04, 0x00, 0x00, 0x00]
        let codePage = TDSCollation.codePage(from: collation)
        #expect(codePage == 1256)
    }

    @Test("Greek LCID maps to CP1253")
    func greekLCID() {
        // LCID 1032 = 0x0408
        let collation: [UInt8] = [0x08, 0x04, 0x00, 0x00, 0x00]
        let codePage = TDSCollation.codePage(from: collation)
        #expect(codePage == 1253)
    }

    // MARK: - SQL Collations (SortId > 0)

    @Test("SQL SortId 52 (SQL_Latin1_General_CP1_CI_AS) maps to CP1252")
    func sqlLatin1SortId() {
        let collation: [UInt8] = [0x09, 0x04, 0x00, 0x00, 52]
        let codePage = TDSCollation.codePage(from: collation)
        #expect(codePage == 1252)
    }

    @Test("SQL SortId 30 maps to CP437")
    func sqlCP437SortId() {
        let collation: [UInt8] = [0x09, 0x04, 0x00, 0x00, 30]
        let codePage = TDSCollation.codePage(from: collation)
        #expect(codePage == 437)
    }

    @Test("SQL SortId 40 maps to CP850")
    func sqlCP850SortId() {
        let collation: [UInt8] = [0x09, 0x04, 0x00, 0x00, 40]
        let codePage = TDSCollation.codePage(from: collation)
        #expect(codePage == 850)
    }

    // MARK: - String Decoding with Collation

    @Test("Danish characters decode correctly with CP1252 collation")
    func danishCharacterDecoding() {
        // æ = 0xE6, ø = 0xF8, å = 0xE5 in Windows-1252
        let cp1252Bytes: [UInt8] = [0x50, 0x72, 0xF8, 0x73, 0x74, 0xF8]  // "Prøstø"
        let encoding = TDSCollation.encoding(from: [0x06, 0x04, 0x00, 0x00, 0x00])
        let decoded = String(bytes: cp1252Bytes, encoding: encoding)
        #expect(decoded == "Prøstø")
    }

    @Test("All Danish special characters decode from CP1252")
    func allDanishChars() {
        let encoding = TDSCollation.encoding(from: [0x06, 0x04, 0x00, 0x00, 0x00])

        // æ = 0xE6, ø = 0xF8, å = 0xE5 (lowercase)
        // Æ = 0xC6, Ø = 0xD8, Å = 0xC5 (uppercase)
        let bytes: [UInt8] = [0xE6, 0xF8, 0xE5, 0xC6, 0xD8, 0xC5]
        let decoded = String(bytes: bytes, encoding: encoding)
        #expect(decoded == "æøåÆØÅ")
    }

    @Test("Rødgiv decodes correctly from CP1252")
    func rodgivDecoding() {
        // "Rødgiv" — ø is 0xF8 in CP1252
        let bytes: [UInt8] = [0x52, 0xF8, 0x64, 0x67, 0x69, 0x76]
        let encoding = TDSCollation.encoding(from: [0x06, 0x04, 0x00, 0x00, 52])
        let decoded = String(bytes: bytes, encoding: encoding)
        #expect(decoded == "Rødgiv")
    }
}
