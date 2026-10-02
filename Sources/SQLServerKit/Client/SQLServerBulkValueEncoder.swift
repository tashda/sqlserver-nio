import Foundation
import SQLServerTDS

/// Converts a bulk-copy value to its destination column's wire payload, as SqlBulkCopy does on the
/// client: the server takes bulk-load data in the column's own type and does not convert it.
/// Text (what CSV and spreadsheet imports carry) is parsed the way SQL Server would convert it.
enum SQLServerBulkValueEncoder {
    struct ConversionError: Error {
        let reason: String
    }

    static func encode(_ value: SQLServerLiteralValue, for column: TDSColumnMetadata) throws -> [UInt8]? {
        switch value {
        case .null:
            return nil
        case .variant(let inner):
            return try encode(inner, for: column)
        case .raw, .geometry, .geography, .hierarchyID:
            throw ConversionError(reason: "this kind of value cannot be bulk loaded")
        default:
            break
        }

        switch column.dataType {
        case .tinyInt: return try integerBytes(value, width: 1, range: 0...255)
        case .smallInt: return try integerBytes(value, width: 2, range: Int64(Int16.min)...Int64(Int16.max))
        case .int: return try integerBytes(value, width: 4, range: Int64(Int32.min)...Int64(Int32.max))
        case .bigInt: return try integerBytes(value, width: 8, range: Int64.min...Int64.max)
        case .intn:
            switch column.length {
            case 1: return try integerBytes(value, width: 1, range: 0...255)
            case 2: return try integerBytes(value, width: 2, range: Int64(Int16.min)...Int64(Int16.max))
            case 4: return try integerBytes(value, width: 4, range: Int64(Int32.min)...Int64(Int32.max))
            default: return try integerBytes(value, width: 8, range: Int64.min...Int64.max)
            }
        case .bit, .bitn:
            return [try bool(value) ? 1 : 0]
        case .real:
            return littleEndian(Float(try double(value)).bitPattern)
        case .float:
            return littleEndian(try double(value).bitPattern)
        case .floatn:
            let number = try double(value)
            return column.length == 4 ? littleEndian(Float(number).bitPattern) : littleEndian(number.bitPattern)
        case .decimal, .numeric, .decimalLegacy, .numericLegacy:
            return try decimalBytes(try decimalText(value), precision: Int(column.precision), scale: Int(column.scale), length: Int(column.length))
        case .money:
            return moneyBytes(try moneyUnits(value), width: 8)
        case .smallMoney:
            return moneyBytes(try moneyUnits(value), width: 4)
        case .moneyn:
            return moneyBytes(try moneyUnits(value), width: Int(column.length))
        case .guid:
            return guidBytes(try uuid(value))
        case .date:
            return threeBytes(try dateTime(value).days)
        case .time:
            // Rounding up past midnight wraps, as CAST to time does.
            let (_, ticks) = try dateTime(value).daysAndTicks(scale: Int(column.scale))
            return ticksBytes(ticks, scale: Int(column.scale))
        case .datetime2:
            let (days, ticks) = try dateTime(value).daysAndTicks(scale: Int(column.scale))
            return ticksBytes(ticks, scale: Int(column.scale)) + threeBytes(days)
        case .datetimeOffset:
            let parts = try dateTime(value)
            let (days, ticks) = parts.inUTC().daysAndTicks(scale: Int(column.scale))
            return ticksBytes(ticks, scale: Int(column.scale)) + threeBytes(days)
                + littleEndian(Int16(parts.offsetMinutes))
        case .datetime:
            return try datetimeBytes(try dateTime(value))
        case .smallDateTime:
            return try smallDatetimeBytes(try dateTime(value))
        case .datetimen:
            let parts = try dateTime(value)
            return column.length == 4 ? try smallDatetimeBytes(parts) : try datetimeBytes(parts)
        case .char, .varchar, .charLegacy, .varcharLegacy, .text:
            let text = try string(value)
            let encoding = TDSCollation.encoding(from: column.collation)
            guard let data = text.data(using: encoding, allowLossyConversion: true) else {
                throw ConversionError(reason: "cannot be written in the column's code page")
            }
            return try checkedLength(Array(data), column)
        case .nchar, .nvarchar, .nText:
            return try checkedLength(utf16(try string(value)), column)
        case .xml:
            return utf16(try string(value))
        case .binary, .varbinary, .binaryLegacy, .varbinaryLegacy, .image:
            return try checkedLength(try bytes(value), column)
        default:
            throw ConversionError(reason: "the column type \(column.dataType) cannot be bulk loaded")
        }
    }

    // MARK: - Reading values

    private static func text(_ value: SQLServerLiteralValue) -> String? {
        switch value {
        case .string(let s), .nString(let s), .decimal(let s): return s.trimmingCharacters(in: .whitespaces)
        case .int(let n): return String(n)
        case .int64(let n): return String(n)
        case .double(let d): return String(d)
        case .bool(let b): return b ? "1" : "0"
        case .uuid(let u): return u.uuidString
        default: return nil
        }
    }

    private static func string(_ value: SQLServerLiteralValue) throws -> String {
        switch value {
        case .string(let s), .nString(let s), .decimal(let s): return s
        case .date(let date): return iso8601(date)
        case .bytes: throw ConversionError(reason: "binary data is not text")
        default:
            guard let text = text(value) else { throw ConversionError(reason: "is not text") }
            return text
        }
    }

    private static func integerBytes(_ value: SQLServerLiteralValue, width: Int, range: ClosedRange<Int64>) throws -> [UInt8] {
        let number: Int64
        switch value {
        case .int(let n): number = Int64(n)
        case .int64(let n): number = n
        case .bool(let b): number = b ? 1 : 0
        case .double(let d):
            guard d.rounded() == d, let n = Int64(exactly: d) else { throw ConversionError(reason: "is not a whole number") }
            number = n
        default:
            guard let text = text(value), let n = Int64(text) ?? Int64(exactly: Double(text) ?? .nan).flatMap({ Double(text) == Double($0) ? $0 : nil }) else {
                throw ConversionError(reason: "is not a whole number")
            }
            number = n
        }
        guard range.contains(number) else { throw ConversionError(reason: "is out of range") }
        return (0..<width).map { UInt8(truncatingIfNeeded: number >> (8 * $0)) }
    }

    private static func bool(_ value: SQLServerLiteralValue) throws -> Bool {
        switch value {
        case .bool(let b): return b
        case .int(let n): return n != 0
        case .int64(let n): return n != 0
        default:
            switch text(value)?.lowercased() {
            case "1", "true": return true
            case "0", "false": return false
            default: throw ConversionError(reason: "is not a bit (0, 1, true or false)")
            }
        }
    }

    private static func double(_ value: SQLServerLiteralValue) throws -> Double {
        switch value {
        case .double(let d): return d
        case .int(let n): return Double(n)
        case .int64(let n): return Double(n)
        default:
            guard let text = text(value), let d = Double(text), d.isFinite else { throw ConversionError(reason: "is not a number") }
            return d
        }
    }

    private static func decimalText(_ value: SQLServerLiteralValue) throws -> String {
        switch value {
        case .double(let d):
            guard d.isFinite else { throw ConversionError(reason: "is not a number") }
            return String(d)
        default:
            guard let text = text(value) else { throw ConversionError(reason: "is not a number") }
            return text
        }
    }

    private static func uuid(_ value: SQLServerLiteralValue) throws -> UUID {
        if case .uuid(let u) = value { return u }
        guard var text = text(value) else { throw ConversionError(reason: "is not a uniqueidentifier") }
        if text.hasPrefix("{"), text.hasSuffix("}") { text = String(text.dropFirst().dropLast()) }
        guard let u = UUID(uuidString: text) else { throw ConversionError(reason: "is not a uniqueidentifier") }
        return u
    }

    private static func bytes(_ value: SQLServerLiteralValue) throws -> [UInt8] {
        switch value {
        case .bytes(let b): return b
        default:
            guard let text = text(value) else { throw ConversionError(reason: "is not binary data") }
            let hex = text.hasPrefix("0x") || text.hasPrefix("0X") ? String(text.dropFirst(2)) : text
            guard hex.count.isMultiple(of: 2) else { throw ConversionError(reason: "is not hexadecimal binary data") }
            var result: [UInt8] = []
            result.reserveCapacity(hex.count / 2)
            var index = hex.startIndex
            while index < hex.endIndex {
                let next = hex.index(index, offsetBy: 2)
                guard let byte = UInt8(hex[index..<next], radix: 16) else { throw ConversionError(reason: "is not hexadecimal binary data") }
                result.append(byte)
                index = next
            }
            return result
        }
    }

    private static func checkedLength(_ bytes: [UInt8], _ column: TDSColumnMetadata) throws -> [UInt8] {
        let isMax = column.length == 0xFFFF || column.length == -1
        if !isMax, [.char, .varchar, .binary, .varbinary, .nchar, .nvarchar].contains(column.dataType), bytes.count > Int(column.length) {
            let unit = [.nchar, .nvarchar].contains(column.dataType) ? "characters" : "bytes"
            let limit = [.nchar, .nvarchar].contains(column.dataType) ? Int(column.length) / 2 : Int(column.length)
            throw ConversionError(reason: "is longer than the column allows (\(limit) \(unit))")
        }
        return bytes
    }

    // MARK: - Decimal and money

    /// Parses a decimal (optionally with an exponent) into sign and digit string scaled to `scale`,
    /// rounding half away from zero as SQL Server does.
    static func scaledDigits(_ text: String, scale: Int) throws -> (negative: Bool, digits: [UInt8]) {
        var s = Substring(text)
        var negative = false
        if s.first == "-" { negative = true; s = s.dropFirst() } else if s.first == "+" { s = s.dropFirst() }
        var exponent = 0
        if let e = s.firstIndex(where: { $0 == "e" || $0 == "E" }) {
            guard let exp = Int(s[s.index(after: e)...]) else { throw ConversionError(reason: "is not a number") }
            exponent = exp
            s = s[..<e]
        }
        let parts = s.split(separator: ".", omittingEmptySubsequences: false)
        guard parts.count <= 2, !(parts.first ?? "").isEmpty || parts.count == 2 else { throw ConversionError(reason: "is not a number") }
        let integer = String(parts[0])
        let fraction = parts.count == 2 ? String(parts[1]) : ""
        guard (integer + fraction).allSatisfy(\.isASCII), (integer + fraction).allSatisfy(\.isNumber), !(integer + fraction).isEmpty else {
            throw ConversionError(reason: "is not a number")
        }
        var all: [UInt8] = (integer + fraction).compactMap { $0.wholeNumberValue }.map { UInt8($0) }
        // Decimal point position after applying the exponent, relative to the digit list.
        let pointIndex = integer.count + exponent
        var fractionDigits = all.count - pointIndex
        if fractionDigits < 0 {
            all += [UInt8](repeating: 0, count: -fractionDigits)
            fractionDigits = 0
        }
        if fractionDigits > scale {
            let dropped = all[(all.count - (fractionDigits - scale))...]
            all.removeLast(fractionDigits - scale)
            if (dropped.first ?? 0) >= 5 {
                var i = all.count - 1
                while i >= 0 {
                    if all[i] == 9 { all[i] = 0; i -= 1 } else { all[i] += 1; break }
                }
                if i < 0 { all.insert(1, at: 0) }
            }
        } else if fractionDigits < scale {
            all += [UInt8](repeating: 0, count: scale - fractionDigits)
        }
        while all.count > 1, all.first == 0 { all.removeFirst() }
        if all == [0] { negative = false }
        return (negative, all)
    }

    static func decimalBytes(_ text: String, precision: Int, scale: Int, length: Int) throws -> [UInt8] {
        let (negative, digits) = try scaledDigits(text, scale: scale)
        guard digits.count <= max(precision, 1) else { throw ConversionError(reason: "is out of range for decimal(\(precision),\(scale))") }
        var magnitude = [UInt8](repeating: 0, count: max(length - 1, 4))
        for digit in digits {
            var carry = UInt16(digit)
            for i in 0..<magnitude.count {
                let product = UInt16(magnitude[i]) * 10 + carry
                magnitude[i] = UInt8(product & 0xFF)
                carry = product >> 8
            }
            guard carry == 0 else { throw ConversionError(reason: "is out of range") }
        }
        return [negative ? 0 : 1] + magnitude
    }

    private static func moneyUnits(_ value: SQLServerLiteralValue) throws -> Int64 {
        let text: String
        if case .double(let d) = value { text = String(d) } else {
            guard var t = self.text(value) else { throw ConversionError(reason: "is not an amount of money") }
            t = t.replacingOccurrences(of: ",", with: "").replacingOccurrences(of: "$", with: "")
            text = t
        }
        let (negative, digits) = try scaledDigits(text, scale: 4)
        guard digits.count <= 19 else { throw ConversionError(reason: "is out of range for money") }
        var units: Int64 = 0
        for digit in digits {
            let (m, o1) = units.multipliedReportingOverflow(by: 10)
            let (a, o2) = m.addingReportingOverflow(Int64(digit))
            guard !o1, !o2 else { throw ConversionError(reason: "is out of range for money") }
            units = a
        }
        return negative ? -units : units
    }

    private static func moneyBytes(_ units: Int64, width: Int) -> [UInt8] {
        if width == 4 { return littleEndian(Int32(truncatingIfNeeded: units)) }
        // money: the high 32 bits first, then the low 32 bits.
        return littleEndian(Int32(truncatingIfNeeded: units >> 32)) + littleEndian(UInt32(truncatingIfNeeded: units))
    }

    // MARK: - Dates and times

    struct DateTimeParts {
        var year = 1900, month = 1, day = 1
        var hour = 0, minute = 0, second = 0
        /// Fraction of a second in 100-nanosecond units (7 digits).
        var fraction100ns = 0
        var offsetMinutes = 0

        /// Days since 0001-01-01.
        var days: Int {
            let y = month <= 2 ? year - 1 : year
            let era = (y >= 0 ? y : y - 399) / 400
            let yoe = y - era * 400
            let mp = (month + 9) % 12
            let doy = (153 * mp + 2) / 5 + day - 1
            let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy
            return era * 146_097 + doe - 719_468 + 719_162 // days since 1970, then since 0001
        }

        func ticks(scale: Int) -> UInt64 {
            let seconds = UInt64(hour * 3600 + minute * 60 + second)
            let divisor = UInt64(pow10(7 - scale))
            // Round the fraction to the column's scale.
            let fraction = (UInt64(fraction100ns) + divisor / 2) / divisor
            return seconds * UInt64(pow10(scale)) + fraction
        }

        /// Days since 0001-01-01 and time ticks at `scale`, carrying into the next day when the
        /// fraction rounds up to midnight.
        func daysAndTicks(scale: Int) -> (days: Int, ticks: UInt64) {
            let ticks = self.ticks(scale: scale)
            let perDay = UInt64(86_400 * pow10(scale))
            return ticks >= perDay ? (days + 1, ticks - perDay) : (days, ticks)
        }

        /// The same instant in UTC with its offset kept (datetimeoffset stores UTC).
        func inUTC() -> DateTimeParts {
            var parts = self
            var total = (hour * 60 + minute) - offsetMinutes
            var dayShift = 0
            while total < 0 { total += 1440; dayShift -= 1 }
            while total >= 1440 { total -= 1440; dayShift += 1 }
            parts.hour = total / 60
            parts.minute = total % 60
            if dayShift != 0 {
                let shifted = DateTimeParts.from(days: days + dayShift)
                parts.year = shifted.year; parts.month = shifted.month; parts.day = shifted.day
            }
            return parts
        }

        static func from(days: Int) -> DateTimeParts {
            let z = days - 719_162 + 719_468
            let era = (z >= 0 ? z : z - 146_096) / 146_097
            let doe = z - era * 146_097
            let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365
            let doy = doe - (365 * yoe + yoe / 4 - yoe / 100)
            let mp = (5 * doy + 2) / 153
            var parts = DateTimeParts()
            parts.day = doy - (153 * mp + 2) / 5 + 1
            parts.month = mp < 10 ? mp + 3 : mp - 9
            parts.year = yoe + era * 400 + (parts.month <= 2 ? 1 : 0)
            return parts
        }
    }

    private static func pow10(_ n: Int) -> Int { (0..<max(n, 0)).reduce(1) { value, _ in value * 10 } }

    /// Accepts `yyyy-MM-dd`, `yyyy-MM-dd HH:mm[:ss[.fffffff]]` (or `T` between them), a time alone
    /// (`HH:mm[:ss[.fffffff]]`), and an optional `Z` or `±hh:mm` offset.
    static func dateTime(_ value: SQLServerLiteralValue) throws -> DateTimeParts {
        if case .date(let date) = value {
            return try dateTime(.string(iso8601(date)))
        }
        guard let text = text(value) else { throw ConversionError(reason: "is not a date or time") }
        var scanner = DateTimeScanner(text)
        guard let parts = scanner.scan() else {
            throw ConversionError(reason: "is not a date or time (use yyyy-MM-dd HH:mm:ss)")
        }
        guard (1...12).contains(parts.month), (1...31).contains(parts.day), parts.day <= daysIn(parts.month, parts.year),
              (1...9999).contains(parts.year), parts.hour < 24, parts.minute < 60, parts.second < 60 else {
            throw ConversionError(reason: "is not a valid date or time")
        }
        return parts
    }

    /// Reads `[yyyy-MM-dd][( |T)HH:mm[:ss[.fffffff]]][ ][Z|±hh[:]mm]`; at least a date or a time.
    private struct DateTimeScanner {
        private let chars: [Character]
        private var index = 0

        init(_ text: String) { chars = Array(text) }

        private var current: Character? { index < chars.count ? chars[index] : nil }

        private mutating func take(_ c: Character) -> Bool {
            guard current == c else { return false }
            index += 1
            return true
        }

        private mutating func number(min: Int, max: Int) -> (value: Int, digits: String)? {
            var digits = ""
            while digits.count < max, let c = current, c.isASCII, c.isNumber { digits.append(c); index += 1 }
            guard digits.count >= min else { return nil }
            return (Int(digits)!, digits)
        }

        mutating func scan() -> DateTimeParts? {
            var parts = DateTimeParts()
            var hasDate = false, hasTime = false
            let start = index
            if let y = number(min: 4, max: 4), take("-") {
                guard let m = number(min: 1, max: 2), take("-"), let d = number(min: 1, max: 2) else { return nil }
                parts.year = y.value; parts.month = m.value; parts.day = d.value
                hasDate = true
                if !take("T") { _ = take(" ") }
            } else {
                index = start
            }
            let timeStart = index
            if let h = number(min: 1, max: 2), take(":") {
                guard let mi = number(min: 2, max: 2) else { return nil }
                parts.hour = h.value; parts.minute = mi.value
                if take(":") {
                    guard let sec = number(min: 2, max: 2) else { return nil }
                    parts.second = sec.value
                    if take(".") {
                        guard let f = number(min: 1, max: 9) else { return nil }
                        parts.fraction100ns = Int(String(f.digits.prefix(7)).padding(toLength: 7, withPad: "0", startingAt: 0))!
                    }
                }
                hasTime = true
            } else {
                index = timeStart
            }
            guard hasDate || hasTime else { return nil }
            while take(" ") {}
            if take("Z") {
                // UTC
            } else if let sign = current, sign == "+" || sign == "-" {
                index += 1
                guard let oh = number(min: 2, max: 2) else { return nil }
                _ = take(":")
                guard let om = number(min: 2, max: 2) else { return nil }
                let minutes = oh.value * 60 + om.value
                parts.offsetMinutes = sign == "-" ? -minutes : minutes
            }
            return index == chars.count ? parts : nil
        }
    }

    private static func daysIn(_ month: Int, _ year: Int) -> Int {
        switch month {
        case 2: return (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 ? 29 : 28
        case 4, 6, 9, 11: return 30
        default: return 31
        }
    }

    private static func iso8601(_ date: Date) -> String {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        formatter.timeZone = TimeZone(identifier: "UTC")
        return formatter.string(from: date)
    }

    private static func threeBytes(_ days: Int) -> [UInt8] {
        (0..<3).map { UInt8(truncatingIfNeeded: days >> (8 * $0)) }
    }

    private static func ticksBytes(_ ticks: UInt64, scale: Int) -> [UInt8] {
        let width = scale <= 2 ? 3 : scale <= 4 ? 4 : 5
        return (0..<width).map { UInt8(truncatingIfNeeded: ticks >> (8 * $0)) }
    }

    private static func datetimeBytes(_ parts: DateTimeParts) throws -> [UInt8] {
        let days = parts.days - 693_595 // since 1900-01-01
        guard parts.year >= 1753 else { throw ConversionError(reason: "is before 1753, the first year datetime holds") }
        // 1/300 second ticks, rounded from 100 ns units; 23:59:59.999 rounds up to the next day.
        let units = Double((parts.hour * 3600 + parts.minute * 60 + parts.second) * 10_000_000 + parts.fraction100ns)
        let ticks = Int((units * 300 / 10_000_000).rounded())
        let perDay = 86_400 * 300
        return littleEndian(Int32(days + ticks / perDay)) + littleEndian(UInt32(ticks % perDay))
    }

    private static func smallDatetimeBytes(_ parts: DateTimeParts) throws -> [UInt8] {
        var days = parts.days - 693_595
        var minutes = parts.hour * 60 + parts.minute + (parts.second >= 30 ? 1 : 0)
        if minutes == 1440 { minutes = 0; days += 1 }
        guard (0...Int(UInt16.max)).contains(days) else { throw ConversionError(reason: "is out of range for smalldatetime (1900-01-01 to 2079-06-06)") }
        return littleEndian(UInt16(days)) + littleEndian(UInt16(minutes))
    }

    // MARK: - Bytes

    private static func guidBytes(_ uuid: UUID) -> [UInt8] {
        let u = uuid.uuid
        return [u.3, u.2, u.1, u.0, u.5, u.4, u.7, u.6, u.8, u.9, u.10, u.11, u.12, u.13, u.14, u.15]
    }

    private static func utf16(_ text: String) -> [UInt8] {
        text.utf16.flatMap { [UInt8($0 & 0xFF), UInt8($0 >> 8)] }
    }

    private static func littleEndian<T: FixedWidthInteger>(_ value: T) -> [UInt8] {
        withUnsafeBytes(of: value.littleEndian) { Array($0) }
    }
}
