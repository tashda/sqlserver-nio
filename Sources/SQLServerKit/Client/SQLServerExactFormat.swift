import NIOCore
import SQLServerTDS

/// Exact display strings for temporal and money values, computed from the
/// wire bytes. Going through `Date` or `Double` loses fractional seconds,
/// the time zone offset and money digits; SQL Server users expect what
/// `CONVERT(..., 121)` and SSMS show:
///
/// | Type | Example |
/// |---|---|
/// | date | `2026-09-30` |
/// | time(7) | `23:59:59.1234567` |
/// | datetime | `2026-09-30 12:34:56.123` |
/// | smalldatetime | `2026-09-30 12:34:00` |
/// | datetime2(2) | `2026-09-30 12:34:56.12` |
/// | datetimeoffset(7) | `2026-09-30 12:34:56.1234567 +02:00` |
/// | money | `922337203685477.5807` |
enum SQLServerExactFormat {
    /// Days from 0001-01-01 to 1900-01-01 (proleptic Gregorian).
    private static let daysFrom0001To1900 = 693_595
    /// Days from 0001-01-01 to 1970-01-01.
    private static let daysFrom0001To1970 = 719_162

    static func format(metadata: TDSTokens.ColMetadataToken.ColumnData, buffer: ByteBuffer) -> String? {
        var value = buffer
        let scale = Int(min(metadata.scale, 7))
        switch metadata.dataType {
        case .date:
            guard let days = value.readUInt24() else { return nil }
            return dateString(daysSince0001: days)
        case .time:
            guard let ticks = value.readTicks(scale: scale) else { return nil }
            return timeString(ticks: ticks, scale: scale)
        case .datetime2:
            guard let ticks = value.readTicks(scale: scale), let days = value.readUInt24() else { return nil }
            return "\(dateString(daysSince0001: days)) \(timeString(ticks: ticks, scale: scale))"
        case .datetimeOffset:
            guard let utcTicks = value.readTicks(scale: scale), let utcDays = value.readUInt24(),
                  let offset: Int16 = value.readInteger(endianness: .little) else { return nil }
            // The wire carries UTC; SQL Server shows local time with its offset.
            let ticksPerDay = 86_400 * pow10(scale)
            var ticks = Int(utcTicks) + Int(offset) * 60 * pow10(scale)
            var days = utcDays
            while ticks < 0 { ticks += ticksPerDay; days -= 1 }
            while ticks >= ticksPerDay { ticks -= ticksPerDay; days += 1 }
            let sign = offset < 0 ? "-" : "+"
            let minutes = abs(Int(offset))
            return "\(dateString(daysSince0001: days)) \(timeString(ticks: UInt64(ticks), scale: scale)) \(sign)\(pad(minutes / 60, 2)):\(pad(minutes % 60, 2))"
        case .datetime:
            return datetimeString(&value)
        case .smallDateTime:
            return smallDatetimeString(&value)
        case .datetimen:
            switch value.readableBytes {
            case 4: return smallDatetimeString(&value)
            case 8: return datetimeString(&value)
            default: return nil
            }
        case .money:
            return moneyString(&value, length: 8)
        case .smallMoney:
            return moneyString(&value, length: 4)
        case .moneyn:
            return moneyString(&value, length: value.readableBytes)
        default:
            return nil
        }
    }

    // MARK: - Pieces

    private static func datetimeString(_ value: inout ByteBuffer) -> String? {
        guard let days: Int32 = value.readInteger(endianness: .little),
              let ticks300: UInt32 = value.readInteger(endianness: .little) else { return nil }
        // 1/300 s ticks, shown to the millisecond as SQL Server rounds them
        // (.000, .003, .007).
        var milliseconds = (Int(ticks300) * 10 + 1) / 3
        var dayNumber = Int(days) + daysFrom0001To1900
        if milliseconds >= 86_400_000 { milliseconds -= 86_400_000; dayNumber += 1 }
        let ticks = UInt64(milliseconds)
        return "\(dateString(daysSince0001: dayNumber)) \(timeString(ticks: ticks, scale: 3))"
    }

    private static func smallDatetimeString(_ value: inout ByteBuffer) -> String? {
        guard let days: UInt16 = value.readInteger(endianness: .little),
              let minutes: UInt16 = value.readInteger(endianness: .little) else { return nil }
        return "\(dateString(daysSince0001: Int(days) + daysFrom0001To1900)) \(timeString(ticks: UInt64(minutes) * 60, scale: 0))"
    }

    private static func moneyString(_ value: inout ByteBuffer, length: Int) -> String? {
        let units: Int64
        switch length {
        case 4:
            guard let v: Int32 = value.readInteger(endianness: .little) else { return nil }
            units = Int64(v)
        case 8:
            // Two 32-bit halves, most significant first.
            guard let high: Int32 = value.readInteger(endianness: .little),
                  let low: UInt32 = value.readInteger(endianness: .little) else { return nil }
            units = Int64(high) << 32 | Int64(low)
        default:
            return nil
        }
        let magnitude = units.magnitude
        let sign = units < 0 ? "-" : ""
        return "\(sign)\(magnitude / 10_000).\(pad(Int(magnitude % 10_000), 4))"
    }

    static func dateString(daysSince0001 days: Int) -> String {
        // Howard Hinnant's civil_from_days, shifted to 0001-01-01.
        let z = days - daysFrom0001To1970 + 719_468
        let era = (z >= 0 ? z : z - 146_096) / 146_097
        let doe = z - era * 146_097
        let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100)
        let mp = (5 * doy + 2) / 153
        let day = doy - (153 * mp + 2) / 5 + 1
        let month = mp < 10 ? mp + 3 : mp - 9
        let year = yoe + era * 400 + (month <= 2 ? 1 : 0)
        return "\(pad(year, 4))-\(pad(month, 2))-\(pad(day, 2))"
    }

    static func timeString(ticks: UInt64, scale: Int) -> String {
        let unit = UInt64(pow10(scale))
        let seconds = Int(ticks / unit)
        let fraction = Int(ticks % unit)
        let base = "\(pad(seconds / 3600, 2)):\(pad(seconds / 60 % 60, 2)):\(pad(seconds % 60, 2))"
        return scale == 0 ? base : "\(base).\(pad(fraction, scale))"
    }

    private static func pow10(_ exponent: Int) -> Int {
        var result = 1
        for _ in 0..<exponent { result *= 10 }
        return result
    }

    private static func pad(_ value: Int, _ width: Int) -> String {
        let digits = String(value)
        return digits.count >= width ? digits : String(repeating: "0", count: width - digits.count) + digits
    }
}

private extension ByteBuffer {
    mutating func readUInt24() -> Int? {
        guard let bytes = readBytes(length: 3) else { return nil }
        return Int(bytes[0]) | Int(bytes[1]) << 8 | Int(bytes[2]) << 16
    }

    /// TIME ticks: 3, 4 or 5 little-endian bytes depending on the scale.
    mutating func readTicks(scale: Int) -> UInt64? {
        let length = scale <= 2 ? 3 : (scale <= 4 ? 4 : 5)
        guard let bytes = readBytes(length: length) else { return nil }
        var ticks: UInt64 = 0
        for (index, byte) in bytes.enumerated() { ticks |= UInt64(byte) << (8 * UInt64(index)) }
        return ticks
    }
}
