import Foundation

/// Extracts the Windows code page from a TDS 5-byte collation structure and returns
/// the corresponding `String.Encoding` for decoding varchar/char data.
///
/// Collation structure (5 bytes):
/// - Bytes 0–2, bits 0–19: LCID (Windows Locale ID)
/// - Bytes 2–3, bits 20–31: ColFlags (case/accent sensitivity)
/// - Byte 4: SortId (SQL sort order ID)
///
/// When SortId > 0, the code page is determined by the SQL sort order.
/// When SortId == 0, the code page is determined by the LCID (Windows collation).
public enum TDSCollation {

    /// Returns the `String.Encoding` for the given 5-byte TDS collation.
    /// Falls back to `.windowsCP1252` if the collation is empty or unmapped.
    public static func encoding(from collation: [UInt8]) -> String.Encoding {
        let codePage = codePage(from: collation)
        return encoding(forCodePage: codePage)
    }

    /// Extracts the Windows code page number from a 5-byte collation.
    public static func codePage(from collation: [UInt8]) -> UInt32 {
        guard collation.count == 5 else { return 1252 }

        let sortId = collation[4]

        // SQL collations: SortId > 0 maps directly to a code page
        if sortId > 0 {
            return codePageForSortId(sortId)
        }

        // Windows collations: derive code page from LCID
        let lcid = UInt32(collation[0])
            | (UInt32(collation[1]) << 8)
            | (UInt32(collation[2] & 0x0F) << 16)  // bits 0-19 only

        return codePageForLCID(lcid)
    }

    // MARK: - SQL Sort Order → Code Page

    /// Maps a SQL Server sort order ID to its corresponding Windows code page.
    /// Reference: MS-TDS 2.2.5.1.2 and SQL Server sys.fn_helpcollations() output.
    private static func codePageForSortId(_ sortId: UInt8) -> UInt32 {
        switch sortId {
        // Code page 437 (US English OEM)
        case 30...33: return 437
        case 34...37: return 437

        // Code page 850 (Multilingual Latin I)
        case 40...44: return 850
        case 45...49: return 850

        // Code page 1252 (Latin I / Western European)
        case 50...56: return 1252
        case 57...61: return 1252
        case 71...72: return 1252

        // Code page 1250 (Central/Eastern European)
        case 80...85: return 1250

        // Code page 1251 (Cyrillic)
        case 104...108: return 1251

        // Code page 932 (Japanese Shift-JIS)
        case 112...113: return 932

        // Code page 936 (Simplified Chinese GBK)
        case 114...120: return 936

        // Code page 949 (Korean)
        case 128...137: return 949

        // Code page 950 (Traditional Chinese Big5)
        case 144...148: return 950

        // Code page 874 (Thai)
        case 152...153: return 874

        // Code page 1253 (Greek)
        case 183...188: return 1253

        // Code page 1254 (Turkish)
        case 190...195: return 1254

        // Code page 1255 (Hebrew)
        case 196...198: return 1255

        // Code page 1256 (Arabic)
        case 200...203: return 1256

        // Code page 1257 (Baltic)
        case 204...206: return 1257

        default:
            // Unknown sort ID — fall back to Latin I
            return 1252
        }
    }

    // MARK: - LCID → Code Page

    /// Maps a Windows Locale ID (primary language) to the default ANSI code page.
    private static func codePageForLCID(_ lcid: UInt32) -> UInt32 {
        // Extract primary language ID (low 10 bits of LCID)
        let primaryLang = lcid & 0x3FF

        switch primaryLang {
        // Latin I / Western European (CP 1252)
        case 0x09: return 1252  // English
        case 0x07: return 1252  // German
        case 0x0C: return 1252  // French
        case 0x0A: return 1252  // Spanish
        case 0x10: return 1252  // Italian
        case 0x13: return 1252  // Dutch
        case 0x06: return 1252  // Danish
        case 0x14: return 1252  // Norwegian
        case 0x1D: return 1252  // Swedish
        case 0x0B: return 1252  // Finnish
        case 0x08: return 1253  // Greek
        case 0x16: return 1252  // Portuguese
        case 0x03: return 1252  // Catalan
        case 0x26: return 1257  // Latvian
        case 0x27: return 1257  // Lithuanian
        case 0x25: return 1257  // Estonian
        case 0x2D: return 1252  // Basque
        case 0x36: return 1252  // Afrikaans
        case 0x21: return 1252  // Indonesian
        case 0x38: return 1252  // Faeroese
        case 0x3E: return 1252  // Malay
        case 0x41: return 1252  // Swahili
        case 0x2A: return 1258  // Vietnamese
        case 0x56: return 1252  // Galician

        // Central/Eastern European (CP 1250)
        case 0x15: return 1250  // Polish
        case 0x05: return 1250  // Czech
        case 0x1B: return 1250  // Slovak
        case 0x0E: return 1250  // Hungarian
        case 0x1A: return 1250  // Croatian/Serbian Latin/Bosnian
        case 0x24: return 1250  // Slovenian
        case 0x18: return 1250  // Romanian
        case 0x1C: return 1250  // Albanian

        // Cyrillic (CP 1251)
        case 0x19: return 1251  // Russian
        case 0x22: return 1251  // Ukrainian
        case 0x23: return 1251  // Belarusian
        case 0x02: return 1251  // Bulgarian
        case 0x2F: return 1251  // Macedonian
        case 0x50: return 1251  // Mongolian (Cyrillic)

        // Turkish (CP 1254)
        case 0x1F: return 1254  // Turkish
        case 0x2C: return 1254  // Azerbaijani (Latin)

        // Hebrew (CP 1255)
        case 0x0D: return 1255  // Hebrew

        // Arabic (CP 1256)
        case 0x01: return 1256  // Arabic

        // Thai (CP 874)
        case 0x1E: return 874   // Thai

        // Japanese (CP 932)
        case 0x11: return 932   // Japanese

        // Korean (CP 949)
        case 0x12: return 949   // Korean

        // Chinese Simplified (CP 936)
        case 0x04: return 936   // Chinese (check sublanguage for Traditional)

        // Hindi / Devanagari
        case 0x39: return 65001 // Hindi — use UTF-8

        default:
            return 1252
        }
    }

    // MARK: - Code Page → String.Encoding

    /// Converts a Windows code page number to `String.Encoding`.
    private static func encoding(forCodePage codePage: UInt32) -> String.Encoding {
        // UTF-8 shortcut
        if codePage == 65001 { return .utf8 }

        #if os(macOS) || os(iOS) || os(tvOS) || os(watchOS) || os(visionOS)
        // Use CoreFoundation to convert Windows code page → NSStringEncoding
        let cfEncoding = CFStringConvertWindowsCodepageToEncoding(codePage)
        if cfEncoding == kCFStringEncodingInvalidId {
            return .windowsCP1252
        }
        let nsEncoding = CFStringConvertEncodingToNSStringEncoding(cfEncoding)
        return String.Encoding(rawValue: nsEncoding)
        #else
        // Linux/non-Apple fallback: map common Windows code pages manually
        switch codePage {
        case 1250: return .windowsCP1250
        case 1251: return .windowsCP1251
        case 1252: return .windowsCP1252
        case 1253: return .windowsCP1253
        case 1254: return .windowsCP1254
        case 28591: return .isoLatin1
        case 28592: return .isoLatin2
        case 20127: return .ascii
        default: return .windowsCP1252
        }
        #endif
    }
}
