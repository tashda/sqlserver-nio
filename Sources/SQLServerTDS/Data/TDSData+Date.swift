import Foundation
import NIOCore

/// Date/Times
/// https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/786f5b8a-f87d-4980-9070-b9b7274c681d

extension TDSData {
    public init(date: Date) {
        var buffer = ByteBufferAllocator().buffer(capacity: 0)
        buffer.writeDatetimeOffset(date: date)
        self.init(metadata: Date.tdsMetadata, value: buffer)
    }

    public var date: Date? {
        if self.metadata.dataType == .sqlVariant {
            return self.sqlVariantResolved()?.date
        }
        guard var value = self.value else {
            return nil
        }

        switch self.metadata.dataType {
        case .smallDateTime:
            guard
                value.readableBytes == 4,
                let daysSince1900 = value.readInteger(endianness: .little, as: UInt16.self),
                let minutesElapsed = value.readInteger(endianness: .little, as: UInt16.self)
            else {
                return nil
            }
            let secondsSinceUnixEpoch =
                Int64(daysSince1900) * _secondsInDay +
                Int64(minutesElapsed) * 60 -
                _secondsBetween1900AndUnixEpoch
            return Date(timeIntervalSince1970: Double(secondsSinceUnixEpoch))

        case .datetime:
            guard
                value.readableBytes == 8,
                let daysSince1900 = value.readInteger(endianness: .little, as: Int32.self),
                let ticks300 = value.readInteger(endianness: .little, as: UInt32.self)
            else {
                return nil
            }
            let dayPart = Double(Int64(daysSince1900) * _secondsInDay) - Double(_secondsBetween1900AndUnixEpoch)
            let timePart = Double(ticks300) / 300.0
            return Date(timeIntervalSince1970: dayPart + timePart)

        case .date:
            return value.readDateAsDate()

        case .time:
            // time alone cannot be accurately represented with Swift's Date type
            return nil

        case .datetime2:
            // datetime2(n): time(n) concatenated with date(3)
            return value.readDatetime2(timeBytes: value.readableBytes - 3, scale: Int(metadata.scale))

        case .datetimeOffset:
            // datetimeoffset(n): time(n) + date(3) + tz-offset(2) where tz-offset is minutes from UTC.
            // The stored time/date are LOCAL to the given timezone; subtract offset to get UTC.
            guard
                let localDatetime = value.readDatetime2(timeBytes: value.readableBytes - 5, scale: Int(metadata.scale)),
                let timezoneOffset = value.readInteger(endianness: .little, as: Int16.self),
                timezoneOffset >= -840 && timezoneOffset <= 840
            else {
                return nil
            }
            return localDatetime.addingTimeInterval(-Double(timezoneOffset) * 60.0)

        default:
            return nil
        }
    }
}

extension ByteBuffer {
    fileprivate mutating func readByteLengthInteger<T: FixedWidthInteger>(length: Int) -> T? {
        guard length > 0, let bytes = readBytes(length: length) else { return nil }
        return bytes.enumerated().reduce(T.zero) { partial, pair in
            partial | (T(pair.element) << T(pair.offset * 8))
        }
    }

    /// Encodes a Swift Date as a TDS datetimeoffset(7) value (UTC, tz-offset=0).
    /// Wire layout: 5 bytes time ticks (LE, 100ns resolution) + 3 bytes date (LE, days since Jan 1 year 1) + 2 bytes tz offset (LE, minutes).
    fileprivate mutating func writeDatetimeOffset(date: Date) {
        let unixSeconds = date.timeIntervalSince1970
        let daysSinceUnixEpoch = Int64(floor(unixSeconds / 86400.0))
        let secondsSinceMidnight = unixSeconds - Double(daysSinceUnixEpoch) * 86400.0

        guard secondsSinceMidnight >= 0 && secondsSinceMidnight < 86400.0 else { return }

        let ticks = UInt64(secondsSinceMidnight * 10_000_000)
        guard ticks < (1 << 40) else { return }

        let daysSinceJan1 = daysSinceUnixEpoch + _daysBetweenEraStartAndUnixEpoch
        guard daysSinceJan1 >= 0 && daysSinceJan1 < (1 << 24) else { return }

        for shift in stride(from: 0, to: 40, by: 8) {
            writeInteger(UInt8(truncatingIfNeeded: ticks >> shift))
        }
        for shift in stride(from: 0, to: 24, by: 8) {
            writeInteger(UInt8(truncatingIfNeeded: UInt64(daysSinceJan1) >> shift))
        }
        writeInteger(Int16(0), endianness: .little)
    }

    /// Decodes a TDS DATE value (3-byte unsigned integer, days since Jan 1 year 1).
    fileprivate mutating func readDateAsDate() -> Date? {
        guard let daysSinceJan1: UInt32 = readByteLengthInteger(length: 3) else { return nil }
        let daysSinceUnixEpoch = Int64(daysSinceJan1) - _daysBetweenEraStartAndUnixEpoch
        return Date(timeIntervalSince1970: Double(daysSinceUnixEpoch) * 86_400.0)
    }

    /// Decodes a TDS datetime2(n) value: `timeBytes` bytes of time ticks followed by 3 bytes of date.
    /// Returns the UTC instant represented by the stored local time (no timezone adjustment).
    fileprivate mutating func readDatetime2(timeBytes length: Int, scale: Int?) -> Date? {
        guard var rawTicks: Int = readByteLengthInteger(length: length),
              let scale = scale else { return nil }

        if scale < 7 {
            for _ in scale..<7 { rawTicks *= 10 }
        }

        guard let daysSinceJan1: UInt32 = readByteLengthInteger(length: 3) else { return nil }

        let secondsSinceMidnight = Double(rawTicks) / 10_000_000.0
        let daysSinceUnixEpoch = Int64(daysSinceJan1) - _daysBetweenEraStartAndUnixEpoch
        return Date(timeIntervalSince1970: Double(daysSinceUnixEpoch) * 86_400.0 + secondsSinceMidnight)
    }
}

extension Date: TDSDataConvertible {
    public static var tdsMetadata: any Metadata {
        TypeMetadata(dataType: .datetimeOffset, scale: 7)
    }

    public init?(tdsData: TDSData) {
        guard let date = tdsData.date else {
            return nil
        }
        self = date
    }

    public var tdsData: TDSData? {
        .init(date: self)
    }
}

private let _secondsInDay: Int64 = 24 * 60 * 60
private let _secondsBetween1900AndUnixEpoch: Int64 = 25_567 * _secondsInDay
private let _daysBetweenEraStartAndUnixEpoch: Int64 = 719_162
