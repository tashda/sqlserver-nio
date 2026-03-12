import Foundation
#if canImport(Darwin)
import Darwin
#elseif canImport(Glibc)
import Glibc
#endif
#if canImport(XCTest)
import XCTest
#endif

/// A utility to manage a SQL Server instance via Docker for integration testing.
public class SQLServerDockerManager: @unchecked Sendable {
    public static let shared = SQLServerDockerManager()
    
    public var version: String {
        environmentValue("TDS_VERSION") ?? "2022-latest"
    }

    public var port: Int {
        Int(environmentValue("TDS_DOCKER_PORT") ?? "14331") ?? 14331
    }

    public let password = "Password123!"
    public let username = "sa"
    public let database = "master"
    
    private var containerId: String?
    private let lock = NSLock()
    private var isStarted = false
    private var ownsContainer = false
    private var sqlcmdPath: String?
    private var startedVersion: String?
    private var startedPort: Int?

    private var containerName: String {
        "sqlserver-nio-test-\(normalizedVersion)-agent-\(port)"
    }

    private var normalizedVersion: String {
        version.replacingOccurrences(of: "/", with: "-").replacingOccurrences(of: ":", with: "-")
    }

    private var lockFilePath: String {
        (NSTemporaryDirectory() as NSString).appendingPathComponent("sqlserver-nio-docker-\(port).lock")
    }

    private var adventureWorksMarkerPath: String {
        (NSTemporaryDirectory() as NSString).appendingPathComponent("\(containerName)-AdventureWorks.ready")
    }
    
    private init() {}

    private func exportEnvironment() {
        setenv("TDS_HOSTNAME", "127.0.0.1", 1)
        setenv("TDS_PORT", "\(port)", 1)
        setenv("TDS_USERNAME", username, 1)
        setenv("TDS_PASSWORD", password, 1)
        setenv("TDS_DATABASE", database, 1)
        if envFlagEnabled("TDS_LOAD_ADVENTUREWORKS") {
            setenv("TDS_AW_DATABASE", "AdventureWorks", 1)
        }
    }
    
    private func findDockerExecutable() -> String? {
        let commonPaths = ["/usr/local/bin/docker", "/opt/homebrew/bin/docker", "/usr/bin/docker", "/bin/docker"]
        let fm = FileManager.default
        for path in commonPaths { if fm.isExecutableFile(atPath: path) { return path } }
        return nil
    }

    private func createDockerProcess(executable: String, arguments: [String]) -> Process {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: executable)
        process.arguments = arguments
        var env = ProcessInfo.processInfo.environment
        let dockerDir = (executable as NSString).deletingLastPathComponent
        let currentPath = env["PATH"] ?? "/usr/bin:/bin:/usr/sbin:/sbin"
        env["PATH"] = "\(dockerDir):/usr/local/bin:/opt/homebrew/bin:\(currentPath)"
        process.environment = env
        return process
    }
    
    public func startIfNeeded() throws {
        lock.lock()
        defer { lock.unlock() }

        try withCrossProcessLock {
            var reusedExistingContainer = false

            if isStarted, startedVersion == version, startedPort == port {
                exportEnvironment()
                return
            }

            guard let dockerPath = findDockerExecutable() else {
                throw NSError(domain: "SQLServerDockerManager", code: 0, userInfo: [NSLocalizedDescriptionKey: "Docker executable not found."])
            }

            try verifyDockerIsRunning(dockerPath: dockerPath)

            if let existingContainerId = try existingContainerID(named: containerName, dockerPath: dockerPath) {
                self.containerId = existingContainerId
                self.ownsContainer = false
                reusedExistingContainer = true
                print("♻️ Reusing SQL Server \(version) container \(existingContainerId) on port \(port).")
            } else {
                try stopContainersSharingPort(dockerPath: dockerPath)
                try startFreshContainer(dockerPath: dockerPath)
            }

            try waitForReady(dockerPath: dockerPath)
            self.sqlcmdPath = try detectSQLCmdPath(dockerPath: dockerPath)

            if let compatibilityLevel = compatibilityLevel(for: version) {
                try setCompatibilityLevel(compatibilityLevel, database: "master", dockerPath: dockerPath)
            }

            if envFlagEnabled("TDS_LOAD_ADVENTUREWORKS"),
               !FileManager.default.fileExists(atPath: adventureWorksMarkerPath) {
                if reusedExistingContainer {
                    print("♻️ Assuming AdventureWorks is already present in reused container \(containerName).")
                } else {
                    try loadAdventureWorks(dockerPath: dockerPath)
                }
                FileManager.default.createFile(atPath: adventureWorksMarkerPath, contents: Data(), attributes: nil)
            }

            exportEnvironment()

            isStarted = true
            startedVersion = version
            startedPort = port
        }
    }
    
    private func resolvedImageName(for version: String) -> String {
        if version.contains("/") {
            return version
        }
        if compatibilityLevel(for: version) != nil {
            print("⚠️ Warning: No official Linux image for \(version). Using SQL Server 2017 with compatibility level emulation.")
            return "mcr.microsoft.com/mssql/server:2017-latest"
        }
        return "mcr.microsoft.com/mssql/server:\(version)"
    }

    private func compatibilityLevel(for version: String) -> Int? {
        switch version {
        case let value where value.hasPrefix("2008"):
            return 100
        case let value where value.hasPrefix("2012"):
            return 110
        case let value where value.hasPrefix("2014"):
            return 120
        case let value where value.hasPrefix("2016"):
            return 130
        default:
            return nil
        }
    }

    private func setCompatibilityLevel(_ level: Int, database: String, dockerPath: String) throws {
        print("⚙️ Setting \(database) compatibility level to \(level)...")
        try runSQL("ALTER DATABASE [\(database)] SET COMPATIBILITY_LEVEL = \(level);", dockerPath: dockerPath)
    }

    public func stop() {
        lock.lock()
        defer { lock.unlock() }
        stopLocked()
    }

    private func stopLocked() {
        guard ownsContainer else {
            self.containerId = nil
            self.isStarted = false
            self.startedVersion = nil
            self.startedPort = nil
            return
        }
        guard let dockerPath = findDockerExecutable() else { return }
        let target = containerId ?? containerName
        print("🛑 Stopping container \(target)...")
        let process = createDockerProcess(executable: dockerPath, arguments: ["stop", target])
        try? process.run(); process.waitUntilExit()
        self.containerId = nil
        self.isStarted = false
        self.ownsContainer = false
        self.startedVersion = nil
        self.startedPort = nil
    }
    
    private func waitForReady(dockerPath: String) throws {
        print("⏳ Waiting for SQL Server...")
        for i in 1...60 {
            let process = createDockerProcess(executable: dockerPath, arguments: ["exec", containerId!, "/bin/bash", "-lc", "if [ -x /opt/mssql-tools18/bin/sqlcmd ]; then /opt/mssql-tools18/bin/sqlcmd -S localhost -U \(username) -P '\(password)' -C -Q \"SELECT 1\"; else /opt/mssql-tools/bin/sqlcmd -S localhost -U \(username) -P '\(password)' -Q \"SELECT 1\"; fi"])
            process.standardOutput = FileHandle.nullDevice; process.standardError = FileHandle.nullDevice
            try? process.run(); process.waitUntilExit()
            if process.terminationStatus == 0 {
                print("✅ Ready after \(i)s.")
                return
            }
            Thread.sleep(forTimeInterval: 1.0)
        }
        throw NSError(domain: "SQLServerDockerManager", code: 2, userInfo: [NSLocalizedDescriptionKey: "Timed out waiting for SQL Server."])
    }

    private func detectSQLCmdPath(dockerPath: String) throws -> String {
        let process = createDockerProcess(executable: dockerPath, arguments: ["exec", containerId!, "/bin/bash", "-lc", "if [ -x /opt/mssql-tools18/bin/sqlcmd ]; then echo /opt/mssql-tools18/bin/sqlcmd; elif [ -x /opt/mssql-tools/bin/sqlcmd ]; then echo /opt/mssql-tools/bin/sqlcmd; fi"])
        let output = Pipe()
        process.standardOutput = output
        process.standardError = FileHandle.nullDevice
        try process.run()
        process.waitUntilExit()

        let path = String(data: output.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8)?
            .trimmingCharacters(in: .whitespacesAndNewlines)
        guard process.terminationStatus == 0, let path, !path.isEmpty else {
            throw NSError(domain: "SQLServerDockerManager", code: 4, userInfo: [NSLocalizedDescriptionKey: "sqlcmd was not found inside the SQL Server container."])
        }
        return path
    }

    private func verifyDockerIsRunning(dockerPath: String) throws {
        let checkProcess = createDockerProcess(executable: dockerPath, arguments: ["info"])
        checkProcess.standardOutput = FileHandle.nullDevice
        checkProcess.standardError = FileHandle.nullDevice
        try checkProcess.run()
        checkProcess.waitUntilExit()
        guard checkProcess.terminationStatus == 0 else {
            throw NSError(domain: "SQLServerDockerManager", code: 0, userInfo: [NSLocalizedDescriptionKey: "Docker is not running."])
        }
    }

    private func existingContainerID(named name: String, dockerPath: String) throws -> String? {
        let process = createDockerProcess(executable: dockerPath, arguments: ["ps", "-aq", "--filter", "name=^\(name)$"])
        let output = Pipe()
        process.standardOutput = output
        process.standardError = FileHandle.nullDevice
        try process.run()
        process.waitUntilExit()
        guard process.terminationStatus == 0 else { return nil }
        let containerID = String(data: output.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8)?
            .trimmingCharacters(in: .whitespacesAndNewlines)
        guard let containerID, !containerID.isEmpty else { return nil }
        return containerID
    }

    private func stopContainersSharingPort(dockerPath: String) throws {
        let process = createDockerProcess(executable: dockerPath, arguments: ["ps", "-aq", "--filter", "name=^sqlserver-nio-test-.*-\(port)$"])
        let output = Pipe()
        process.standardOutput = output
        process.standardError = FileHandle.nullDevice
        try process.run()
        process.waitUntilExit()
        guard process.terminationStatus == 0 else { return }
        let identifiers = String(data: output.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8)?
            .split(whereSeparator: \.isNewline)
            .map(String.init) ?? []
        for identifier in identifiers where !identifier.isEmpty {
            let stop = createDockerProcess(executable: dockerPath, arguments: ["rm", "-f", identifier])
            stop.standardOutput = FileHandle.nullDevice
            stop.standardError = FileHandle.nullDevice
            try? stop.run()
            stop.waitUntilExit()
        }
        try? FileManager.default.removeItem(atPath: adventureWorksMarkerPath)
    }

    private func startFreshContainer(dockerPath: String) throws {
        print("🚀 Starting SQL Server \(version) on port \(port)...")
        let architecture = ProcessInfo.processInfo.machineArchitecture
        let platformArguments = architecture.contains("arm64") || architecture.contains("aarch64") ? ["--platform", "linux/amd64"] : []
        try? FileManager.default.removeItem(atPath: adventureWorksMarkerPath)

        let process = createDockerProcess(executable: dockerPath, arguments: ["run"] + platformArguments + [
            "-d", "--rm",
            "--name", containerName,
            "-p", "\(port):1433",
            "-e", "ACCEPT_EULA=Y",
            "-e", "MSSQL_SA_PASSWORD=\(password)",
            "-e", "MSSQL_AGENT_ENABLED=true",
            resolvedImageName(for: version)
        ])

        let outPipe = Pipe()
        let errPipe = Pipe()
        process.standardOutput = outPipe
        process.standardError = errPipe
        try process.run()
        process.waitUntilExit()

        let output = String(data: outPipe.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8)?
            .trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        if process.terminationStatus != 0 || output.isEmpty {
            let errorOutput = String(data: errPipe.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8) ?? ""
            throw NSError(domain: "SQLServerDockerManager", code: 1, userInfo: [NSLocalizedDescriptionKey: "Failed to start Docker container: \(errorOutput)"])
        }

        self.containerId = output
        self.ownsContainer = true
        print("📦 Container started: \(output)")
    }

    private func withCrossProcessLock<T>(_ body: () throws -> T) throws -> T {
        let fileDescriptor = open(lockFilePath, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR)
        guard fileDescriptor >= 0 else {
            throw NSError(domain: "SQLServerDockerManager", code: 10, userInfo: [NSLocalizedDescriptionKey: "Failed to open Docker lock file at \(lockFilePath)."])
        }
        defer { close(fileDescriptor) }
        guard flock(fileDescriptor, LOCK_EX) == 0 else {
            throw NSError(domain: "SQLServerDockerManager", code: 11, userInfo: [NSLocalizedDescriptionKey: "Failed to lock Docker coordination file at \(lockFilePath)."])
        }
        defer { flock(fileDescriptor, LOCK_UN) }
        return try body()
    }
    
    private func loadAdventureWorks(dockerPath: String) throws {
        print("📄 Restoring AdventureWorks...")
        
        let awVersion: String
        if version.contains("2025") { awVersion = "2025" }
        else if version.contains("2022") { awVersion = "2022" }
        else if version.contains("2019") { awVersion = "2019" }
        else if version.contains("2017") { awVersion = "2017" }
        else if version.contains("2016") { awVersion = "2016" }
        else if version.contains("2014") { awVersion = "2014" }
        else if version.contains("2012") { awVersion = "2012" }
        else { awVersion = "2012" } // 2008 R2 fallback
        
        let bakFilename = "AdventureWorks\(awVersion).bak"
        let url = "https://github.com/microsoft/sql-server-samples/releases/download/adventureworks/\(bakFilename)"
        let localBakPath = (NSTemporaryDirectory() as NSString).appendingPathComponent(bakFilename)
        
        if !FileManager.default.fileExists(atPath: localBakPath) {
            print("📥 Downloading \(url)...")
            let curl = Process()
            curl.executableURL = URL(fileURLWithPath: "/usr/bin/curl")
            curl.arguments = ["-L", "-o", localBakPath, url]
            try curl.run(); curl.waitUntilExit()
        }

        print("📦 Transferring backup to container...")
        let cp = createDockerProcess(executable: dockerPath, arguments: ["cp", localBakPath, "\(containerId!):/var/opt/mssql/data/AdventureWorks.bak"])
        try cp.run(); cp.waitUntilExit()

        print("🔄 Executing RESTORE...")
        let restoreSql = """
        USE master;
        DECLARE @Table TABLE (LogicalName nvarchar(128), PhysicalName nvarchar(128), [Type] char(1), FileGroupName nvarchar(128), Size numeric(20,0), MaxSize numeric(20,0), FileId bigint, CreateLSN numeric(25,0), DropLSN numeric(25,0), UniqueId uniqueidentifier, ReadOnlyLSN numeric(25,0), ReadWriteLSN numeric(25,0), BackupSizeInBytes bigint, SourceBlockSize int, FileGroupId int, LogGroupGUID uniqueidentifier, DifferentialBaseLSN numeric(25,0), DifferentialBaseGUID uniqueidentifier, IsReadOnly bit, IsPresent bit, TDEThumbprint varbinary(32), SnapshotURL nvarchar(360))
        INSERT INTO @Table EXEC('RESTORE FILELISTONLY FROM DISK = ''/var/opt/mssql/data/AdventureWorks.bak''')
        DECLARE @Data nvarchar(128) = (SELECT TOP 1 LogicalName FROM @Table WHERE [Type] = 'D' ORDER BY FileId)
        DECLARE @Log nvarchar(128) = (SELECT TOP 1 LogicalName FROM @Table WHERE [Type] = 'L')
        DECLARE @Restore nvarchar(max) = 'RESTORE DATABASE AdventureWorks FROM DISK = ''/var/opt/mssql/data/AdventureWorks.bak'' WITH MOVE ''' + @Data + ''' TO ''/var/opt/mssql/data/AdventureWorks.mdf'', MOVE ''' + @Log + ''' TO ''/var/opt/mssql/data/AdventureWorks.ldf'', REPLACE'
        EXEC(@Restore)
        """
        
        try runSQL(restoreSql, dockerPath: dockerPath)
        if let compatibilityLevel = compatibilityLevel(for: version) {
            try setCompatibilityLevel(compatibilityLevel, database: "AdventureWorks", dockerPath: dockerPath)
        }
        print("✅ AdventureWorks restored.")
    }
    
    public func runSQL(_ sql: String, dockerPath: String) throws {
        let sqlcmdPath = self.sqlcmdPath ?? "/opt/mssql-tools/bin/sqlcmd"
        var arguments = ["exec", "-i", containerId!, sqlcmdPath, "-S", "localhost", "-U", username, "-P", password]
        if sqlcmdPath.contains("mssql-tools18") {
            arguments.append("-C")
        }
        arguments += ["-d", "master"]
        let process = createDockerProcess(executable: dockerPath, arguments: arguments)
        let inputPipe = Pipe(); process.standardInput = inputPipe
        try process.run()
        if let data = sql.data(using: .utf8) { inputPipe.fileHandleForWriting.write(data) }
        try inputPipe.fileHandleForWriting.close()
        process.waitUntilExit()
        if process.terminationStatus != 0 {
            throw NSError(domain: "SQLServerDockerManager", code: 3, userInfo: [NSLocalizedDescriptionKey: "SQL execution failed."])
        }
    }
}

private extension ProcessInfo {
    var machineArchitecture: String {
        var systemInfo = utsname()
        uname(&systemInfo)
        return withUnsafePointer(to: &systemInfo.machine) {
            $0.withMemoryRebound(to: CChar.self, capacity: Int(_SYS_NAMELEN)) {
                String(cString: $0)
            }
        }
    }
}

private func environmentValue(_ key: String) -> String? {
    if let value = getenv(key) {
        return String(cString: value)
    }
    return ProcessInfo.processInfo.environment[key]
}
