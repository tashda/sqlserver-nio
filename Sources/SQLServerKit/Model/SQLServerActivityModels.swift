import Foundation

// Activity Monitor public models

public struct SQLServerActivityOptions: Sendable, Equatable {
    public var includeSqlText: Bool
    public var includeQueryPlan: Bool

    public init(includeSqlText: Bool = false, includeQueryPlan: Bool = false) {
        self.includeSqlText = includeSqlText
        self.includeQueryPlan = includeQueryPlan
    }
}

public struct SQLServerActivitySnapshot: Sendable {
    public let capturedAt: Date
    public let processes: [SQLServerProcessInfo]
    public let waits: [SQLServerWaitStat]
    public let waitsDelta: [SQLServerWaitStatDelta]?
    public let fileIO: [SQLServerFileIOStat]
    public let fileIODelta: [SQLServerFileIOStatDelta]?
    public let expensiveQueries: [SQLServerExpensiveQuery]

    public init(
        capturedAt: Date = Date(),
        processes: [SQLServerProcessInfo],
        waits: [SQLServerWaitStat],
        waitsDelta: [SQLServerWaitStatDelta]? = nil,
        fileIO: [SQLServerFileIOStat],
        fileIODelta: [SQLServerFileIOStatDelta]? = nil,
        expensiveQueries: [SQLServerExpensiveQuery]
    ) {
        self.capturedAt = capturedAt
        self.processes = processes
        self.waits = waits
        self.waitsDelta = waitsDelta
        self.fileIO = fileIO
        self.fileIODelta = fileIODelta
        self.expensiveQueries = expensiveQueries
    }
}

public struct SQLServerProcessInfo: Sendable {
    public struct Request: Sendable {
        public let status: String?
        public let command: String?
        public let cpuTimeMs: Int?
        public let totalElapsedMs: Int?
        public let waitType: String?
        public let waitTimeMs: Int?
        public let lastWaitType: String?
        public let blockingSessionId: Int?
        public let databaseId: Int?
        public let startTime: Date?
        public let percentComplete: Double?
        public let sqlText: String?
        public let planXml: String?
    }

    public let sessionId: Int
    public let loginName: String?
    public let hostName: String?
    public let programName: String?
    public let clientNetAddress: String?
    public let sessionStatus: String?
    public let sessionCpuTimeMs: Int?
    public let sessionReads: Int?
    public let sessionWrites: Int?
    public let memoryUsageKB: Int?
    public let request: Request?
}

public struct SQLServerWaitStat: Sendable {
    public let waitType: String
    public let waitingTasksCount: Int
    public let waitTimeMs: Int
    public let signalWaitTimeMs: Int
}

public struct SQLServerWaitStatDelta: Sendable {
    public let waitType: String
    public let waitingTasksCountDelta: Int
    public let waitTimeMsDelta: Int
    public let signalWaitTimeMsDelta: Int
}

public struct SQLServerFileIOStat: Sendable {
    public let databaseId: Int
    public let fileId: Int
    public let databaseName: String?
    public let fileName: String?
    public let numReads: Int
    public let numWrites: Int
    public let bytesRead: Int64
    public let bytesWritten: Int64
    public let ioStallReadMs: Int64
    public let ioStallWriteMs: Int64
}

public struct SQLServerFileIOStatDelta: Sendable {
    public let databaseId: Int
    public let fileId: Int
    public let numReadsDelta: Int
    public let numWritesDelta: Int
    public let bytesReadDelta: Int64
    public let bytesWrittenDelta: Int64
    public let ioStallReadMsDelta: Int64
    public let ioStallWriteMsDelta: Int64
}

public struct SQLServerExpensiveQuery: Sendable {
    public let queryHashHex: String?
    public let executionCount: Int
    public let totalWorkerTime: Int64
    public let totalElapsedTime: Int64
    public let totalLogicalReads: Int64
    public let totalLogicalWrites: Int64
    public let maxWorkerTime: Int64
    public let maxElapsedTime: Int64
    public let lastExecutionTime: Date?
    public let sqlText: String?
    public let planXml: String?
}

