import Foundation
#if canImport(FoundationXML)
import FoundationXML
#endif
import NIO

// MARK: - Extended Events Types

/// An Extended Events session on the server.
public struct SQLServerXESession: Sendable, Equatable, Identifiable {
    public var id: String { name }

    public let name: String
    public let createTime: Date?
    public let startupState: Bool
    public let isRunning: Bool

    public init(
        name: String,
        createTime: Date?,
        startupState: Bool,
        isRunning: Bool
    ) {
        self.name = name
        self.createTime = createTime
        self.startupState = startupState
        self.isRunning = isRunning
    }
}

/// Detailed information about an XE session including its events and targets.
public struct SQLServerXESessionDetail: Sendable, Equatable {
    public let sessionName: String
    public let events: [SQLServerXESessionEvent]
    public let targets: [SQLServerXESessionTarget]

    public init(
        sessionName: String,
        events: [SQLServerXESessionEvent],
        targets: [SQLServerXESessionTarget]
    ) {
        self.sessionName = sessionName
        self.events = events
        self.targets = targets
    }
}

/// An event bound to an XE session.
public struct SQLServerXESessionEvent: Sendable, Equatable, Identifiable {
    public var id: String { eventName }

    public let eventName: String
    public let packageName: String

    public init(eventName: String, packageName: String) {
        self.eventName = eventName
        self.packageName = packageName
    }
}

/// A target bound to an XE session.
public struct SQLServerXESessionTarget: Sendable, Equatable, Identifiable {
    public var id: String { targetName }

    public let targetName: String
    public let targetData: String?

    public init(targetName: String, targetData: String?) {
        self.targetName = targetName
        self.targetData = targetData
    }
}

/// An available XE event from the catalog.
public struct SQLServerXEEvent: Sendable, Equatable, Identifiable {
    public var id: String { "\(packageName).\(eventName)" }

    public let packageName: String
    public let eventName: String
    public let description: String?

    public init(packageName: String, eventName: String, description: String?) {
        self.packageName = packageName
        self.eventName = eventName
        self.description = description
    }
}

/// A captured event data row from a ring buffer target.
public struct SQLServerXEEventData: Sendable, Equatable, Identifiable {
    public let id: UUID
    public let timestamp: Date?
    public let eventName: String
    public let fields: [String: String]

    public init(
        id: UUID = UUID(),
        timestamp: Date?,
        eventName: String,
        fields: [String: String]
    ) {
        self.id = id
        self.timestamp = timestamp
        self.eventName = eventName
        self.fields = fields
    }
}

/// Configuration for creating an XE session.
public struct SQLServerXESessionConfiguration: Sendable {
    public struct EventSpec: Sendable {
        public let eventName: String
        public let actions: [String]
        public let predicate: String?

        public init(eventName: String, actions: [String] = [], predicate: String? = nil) {
            self.eventName = eventName
            self.actions = actions
            self.predicate = predicate
        }
    }

    public enum TargetType: Sendable {
        case ringBuffer(maxMemoryKB: Int)
        case eventFile(filename: String, maxFileSizeMB: Int)
    }

    public let name: String
    public let events: [EventSpec]
    public let target: TargetType
    public let maxMemoryKB: Int
    public let startupState: Bool

    public init(
        name: String,
        events: [EventSpec],
        target: TargetType = .ringBuffer(maxMemoryKB: 4096),
        maxMemoryKB: Int = 4096,
        startupState: Bool = false
    ) {
        self.name = name
        self.events = events
        self.target = target
        self.maxMemoryKB = maxMemoryKB
        self.startupState = startupState
    }
}

// MARK: - SQLServerExtendedEventsClient

/// Namespace client for SQL Server Extended Events operations.
///
/// Extended Events (XE) is the lightweight event tracing system that replaces
/// SQL Trace / SQL Profiler. This client provides typed APIs for managing XE
/// sessions, reading captured event data, and browsing the event catalog.
///
/// Usage:
/// ```swift
/// let sessions = try await client.extendedEvents.listSessions()
/// try await client.extendedEvents.startSession(name: "SlowQueries")
/// let events = try await client.extendedEvents.readRingBufferData(sessionName: "SlowQueries")
/// ```
public final class SQLServerExtendedEventsClient: @unchecked Sendable {
    private let client: SQLServerClient

    internal init(client: SQLServerClient) {
        self.client = client
    }

    // MARK: - List Sessions

    /// Returns all Extended Events sessions defined on the server.
    @available(macOS 12.0, *)
    public func listSessions() async throws -> [SQLServerXESession] {
        let sql = """
        SELECT
            ses.name,
            ses.startup_state,
            CASE WHEN ds.name IS NOT NULL THEN 1 ELSE 0 END AS is_running,
            ds.create_time
        FROM sys.server_event_sessions ses
        LEFT JOIN sys.dm_xe_sessions ds ON ses.name = ds.name
        ORDER BY ses.name
        """
        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard let name = row.column("name")?.string else { return nil }
            return SQLServerXESession(
                name: name,
                createTime: row.column("create_time")?.date,
                startupState: row.column("startup_state")?.bool ?? false,
                isRunning: (row.column("is_running")?.int ?? 0) == 1
            )
        }
    }

    // MARK: - Session Details

    /// Returns detailed information about an XE session including its events and targets.
    @available(macOS 12.0, *)
    public func sessionDetails(name sessionName: String) async throws -> SQLServerXESessionDetail {
        let escapedName = sessionName.replacingOccurrences(of: "'", with: "''")

        let eventsSql = """
        SELECT
            e.name AS [event_name],
            e.package AS [event_package_name]
        FROM sys.server_event_sessions s
        JOIN sys.server_event_session_events e ON s.event_session_id = e.event_session_id
        WHERE s.name = '\(escapedName)'
        ORDER BY e.name
        """

        let targetsSql = """
        SELECT
            t.name AS [target_name]
        FROM sys.server_event_sessions s
        JOIN sys.server_event_session_targets t ON s.event_session_id = t.event_session_id
        WHERE s.name = '\(escapedName)'
        """

        let eventRows = try await client.query(eventsSql)
        let targetRows = try await client.query(targetsSql)

        let events = eventRows.compactMap { row -> SQLServerXESessionEvent? in
            guard let eventName = row.column("event_name")?.string else { return nil }
            return SQLServerXESessionEvent(
                eventName: eventName,
                packageName: row.column("event_package_name")?.string ?? ""
            )
        }

        let targets = targetRows.compactMap { row -> SQLServerXESessionTarget? in
            guard let targetName = row.column("target_name")?.string else { return nil }
            return SQLServerXESessionTarget(
                targetName: targetName,
                targetData: row.column("target_data")?.string
            )
        }

        return SQLServerXESessionDetail(
            sessionName: sessionName,
            events: events,
            targets: targets
        )
    }

    // MARK: - Start / Stop

    /// Starts an Extended Events session.
    @available(macOS 12.0, *)
    public func startSession(name: String) async throws {
        let escapedName = name.replacingOccurrences(of: "]", with: "]]")
        _ = try await client.execute("ALTER EVENT SESSION [\(escapedName)] ON SERVER STATE = START")
    }

    /// Stops an Extended Events session.
    @available(macOS 12.0, *)
    public func stopSession(name: String) async throws {
        let escapedName = name.replacingOccurrences(of: "]", with: "]]")
        _ = try await client.execute("ALTER EVENT SESSION [\(escapedName)] ON SERVER STATE = STOP")
    }

    // MARK: - Ring Buffer Data

    /// Reads captured event data from a ring_buffer target.
    ///
    /// The ring buffer XML is parsed into structured `SQLServerXEEventData` rows.
    /// Returns an empty array if the session has no ring_buffer target or no data.
    @available(macOS 12.0, *)
    public func readRingBufferData(
        sessionName: String,
        maxEvents: Int = 100
    ) async throws -> [SQLServerXEEventData] {
        let escapedName = sessionName.replacingOccurrences(of: "'", with: "''")
        let sql = """
        SELECT CAST(target_data AS NVARCHAR(MAX)) AS target_data
        FROM sys.dm_xe_session_targets t
        JOIN sys.dm_xe_sessions s ON t.event_session_address = s.address
        WHERE s.name = '\(escapedName)' AND t.target_name = 'ring_buffer'
        """
        let rows = try await client.query(sql)
        guard let xmlString = rows.first?.column("target_data")?.string,
              !xmlString.isEmpty else {
            return []
        }

        return parseRingBufferXML(xmlString, maxEvents: maxEvents)
    }

    // MARK: - Available Events Catalog

    /// Returns the catalog of available XE events on the server.
    @available(macOS 12.0, *)
    public func listAvailableEvents() async throws -> [SQLServerXEEvent] {
        let sql = """
        SELECT
            p.name AS package_name,
            o.name AS event_name,
            o.description
        FROM sys.dm_xe_objects o
        JOIN sys.dm_xe_packages p ON o.package_guid = p.guid
        WHERE o.object_type = 'event'
        ORDER BY p.name, o.name
        """
        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard let packageName = row.column("package_name")?.string,
                  let eventName = row.column("event_name")?.string else { return nil }
            return SQLServerXEEvent(
                packageName: packageName,
                eventName: eventName,
                description: row.column("description")?.string
            )
        }
    }

    // MARK: - Create Session

    /// Creates a new Extended Events session.
    @available(macOS 12.0, *)
    public func createSession(_ config: SQLServerXESessionConfiguration) async throws {
        let escapedName = config.name.replacingOccurrences(of: "]", with: "]]")

        var sql = "CREATE EVENT SESSION [\(escapedName)] ON SERVER\n"

        for (index, event) in config.events.enumerated() {
            let prefix = index == 0 ? "ADD EVENT" : ",\nADD EVENT"
            sql += "\(prefix) \(event.eventName)("

            if !event.actions.isEmpty {
                sql += "\n    ACTION(\(event.actions.joined(separator: ", ")))"
            }
            if let predicate = event.predicate, !predicate.isEmpty {
                let separator = event.actions.isEmpty ? "\n    " : "\n    "
                sql += "\(separator)WHERE (\(predicate))"
            }

            sql += ")\n"
        }

        switch config.target {
        case .ringBuffer(let maxMemoryKB):
            sql += "ADD TARGET package0.ring_buffer(SET max_memory=(\(maxMemoryKB)))\n"
        case .eventFile(let filename, let maxFileSizeMB):
            let escapedFile = filename.replacingOccurrences(of: "'", with: "''")
            sql += "ADD TARGET package0.event_file(SET filename=N'\(escapedFile)', max_file_size=(\(maxFileSizeMB)))\n"
        }

        let startupStr = config.startupState ? "ON" : "OFF"
        sql += "WITH (MAX_MEMORY=\(config.maxMemoryKB) KB, EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS, STARTUP_STATE=\(startupStr))"

        _ = try await client.execute(sql)
    }

    // MARK: - Drop Session

    /// Drops (deletes) an Extended Events session.
    @available(macOS 12.0, *)
    public func dropSession(name: String) async throws {
        let escapedName = name.replacingOccurrences(of: "]", with: "]]")
        // Stop first if running, then drop
        do {
            _ = try await client.execute("ALTER EVENT SESSION [\(escapedName)] ON SERVER STATE = STOP")
        } catch {
            // Session may already be stopped — ignore
        }
        _ = try await client.execute("DROP EVENT SESSION [\(escapedName)] ON SERVER")
    }

    // MARK: - XML Parsing

    /// Parses the ring buffer XML into structured event data.
    private func parseRingBufferXML(_ xml: String, maxEvents: Int) -> [SQLServerXEEventData] {
        guard let data = xml.data(using: .utf8) else { return [] }
        let parser = RingBufferXMLParser(maxEvents: maxEvents)
        let xmlParser = XMLParser(data: data)
        xmlParser.delegate = parser
        xmlParser.parse()
        return parser.events
    }

    // MARK: - Alter Session

    /// Adds an event to an existing session. The session must be stopped first.
    @available(macOS 12.0, *)
    public func addEvent(sessionName: String, eventName: String, predicate: String? = nil) async throws {
        let escaped = sessionName.replacingOccurrences(of: "]", with: "]]")
        var sql = "ALTER EVENT SESSION [\(escaped)] ON SERVER ADD EVENT sqlserver.\(eventName)"
        if let predicate, !predicate.isEmpty {
            let escapedPred = predicate.replacingOccurrences(of: "'", with: "''")
            sql += " (WHERE \(escapedPred))"
        }
        sql += ";"
        _ = try await client.execute(sql)
    }

    /// Removes an event from an existing session. The session must be stopped first.
    @available(macOS 12.0, *)
    public func dropEvent(sessionName: String, eventName: String) async throws {
        let escaped = sessionName.replacingOccurrences(of: "]", with: "]]")
        _ = try await client.execute("ALTER EVENT SESSION [\(escaped)] ON SERVER DROP EVENT sqlserver.\(eventName);")
    }
}

// MARK: - Ring Buffer XML Parser

/// Parses SQL Server Extended Events ring_buffer XML into structured data.
///
/// The XML format is:
/// ```xml
/// <RingBufferTarget>
///   <event name="..." timestamp="...">
///     <data name="field_name"><value>field_value</value></data>
///     <action name="action_name"><value>action_value</value></action>
///   </event>
/// </RingBufferTarget>
/// ```
private final class RingBufferXMLParser: NSObject, XMLParserDelegate, @unchecked Sendable {
    let maxEvents: Int
    var events: [SQLServerXEEventData] = []

    private var currentEventName: String?
    private var currentTimestamp: String?
    private var currentFields: [String: String] = [:]
    private var currentFieldName: String?
    private var currentElementName: String?
    private var currentText = ""
    private var inValue = false

    init(maxEvents: Int) {
        self.maxEvents = maxEvents
    }

    func parser(
        _ parser: XMLParser,
        didStartElement elementName: String,
        namespaceURI: String?,
        qualifiedName: String?,
        attributes attributeDict: [String: String]
    ) {
        currentElementName = elementName

        if elementName == "event" {
            currentEventName = attributeDict["name"]
            currentTimestamp = attributeDict["timestamp"]
            currentFields = [:]
        } else if elementName == "data" || elementName == "action" {
            currentFieldName = attributeDict["name"]
        } else if elementName == "value" {
            inValue = true
            currentText = ""
        }
    }

    func parser(_ parser: XMLParser, foundCharacters string: String) {
        if inValue {
            currentText += string
        }
    }

    func parser(
        _ parser: XMLParser,
        didEndElement elementName: String,
        namespaceURI: String?,
        qualifiedName: String?
    ) {
        if elementName == "value" && inValue {
            if let fieldName = currentFieldName {
                currentFields[fieldName] = currentText
            }
            inValue = false
        } else if elementName == "event" {
            guard events.count < maxEvents else {
                parser.abortParsing()
                return
            }
            let timestamp = parseTimestamp(currentTimestamp)
            let event = SQLServerXEEventData(
                timestamp: timestamp,
                eventName: currentEventName ?? "unknown",
                fields: currentFields
            )
            events.append(event)
            currentEventName = nil
            currentTimestamp = nil
            currentFields = [:]
        } else if elementName == "data" || elementName == "action" {
            currentFieldName = nil
        }
    }

    private func parseTimestamp(_ string: String?) -> Date? {
        guard let string else { return nil }
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        if let date = formatter.date(from: string) { return date }
        formatter.formatOptions = [.withInternetDateTime]
        return formatter.date(from: string)
    }
}
