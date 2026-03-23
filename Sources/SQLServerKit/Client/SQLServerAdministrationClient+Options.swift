import Foundation
import SQLServerTDS

// MARK: - ALTER DATABASE SET Options

@available(macOS 12.0, *)
extension SQLServerAdministrationClient {

    /// Alter a database option using ALTER DATABASE SET.
    @discardableResult
    public func alterDatabaseOption(name: String, option: SQLServerDatabaseOption) async throws -> [SQLServerStreamMessage] {
        let escaped = Self.escapeIdentifier(name)
        let onOff: (Bool) -> String = { $0 ? "ON" : "OFF" }
        let setClause: String

        switch option {
        case .recoveryModel(let model):
            setClause = "SET RECOVERY \(model.rawValue)"
        case .compatibilityLevel(let level):
            setClause = "SET COMPATIBILITY_LEVEL = \(level)"
        case .readOnly(let readOnly):
            setClause = readOnly ? "SET READ_ONLY" : "SET READ_WRITE"
        case .autoClose(let on):
            setClause = "SET AUTO_CLOSE \(onOff(on))"
        case .autoShrink(let on):
            setClause = "SET AUTO_SHRINK \(onOff(on))"
        case .autoCreateStatistics(let on):
            setClause = "SET AUTO_CREATE_STATISTICS \(onOff(on))"
        case .autoUpdateStatistics(let on):
            setClause = "SET AUTO_UPDATE_STATISTICS \(onOff(on))"
        case .autoUpdateStatisticsAsync(let on):
            setClause = "SET AUTO_UPDATE_STATISTICS_ASYNC \(onOff(on))"
        case .pageVerify(let option):
            setClause = "SET PAGE_VERIFY \(option.rawValue)"
        case .userAccess(let access):
            setClause = "SET \(access.rawValue)"
        case .targetRecoveryTime(let seconds):
            setClause = "SET TARGET_RECOVERY_TIME = \(seconds) SECONDS"
        case .delayedDurability(let option):
            setClause = "SET DELAYED_DURABILITY = \(option.rawValue)"
        case .allowSnapshotIsolation(let on):
            setClause = "SET ALLOW_SNAPSHOT_ISOLATION \(onOff(on))"
        case .readCommittedSnapshot(let on):
            setClause = "SET READ_COMMITTED_SNAPSHOT \(onOff(on))"
        case .encryption(let on):
            setClause = "SET ENCRYPTION \(onOff(on))"
        case .brokerEnabled(let on):
            setClause = on ? "SET ENABLE_BROKER" : "SET DISABLE_BROKER"
        case .trustworthy(let on):
            setClause = "SET TRUSTWORTHY \(onOff(on))"
        case .parameterization(let option):
            setClause = "SET PARAMETERIZATION \(option.rawValue)"
        case .ansiNullDefault(let on):
            setClause = "SET ANSI_NULL_DEFAULT \(onOff(on))"
        case .ansiNulls(let on):
            setClause = "SET ANSI_NULLS \(onOff(on))"
        case .ansiPadding(let on):
            setClause = "SET ANSI_PADDING \(onOff(on))"
        case .ansiWarnings(let on):
            setClause = "SET ANSI_WARNINGS \(onOff(on))"
        case .arithAbort(let on):
            setClause = "SET ARITHABORT \(onOff(on))"
        case .concatNullYieldsNull(let on):
            setClause = "SET CONCAT_NULL_YIELDS_NULL \(onOff(on))"
        case .quotedIdentifier(let on):
            setClause = "SET QUOTED_IDENTIFIER \(onOff(on))"
        case .recursiveTriggers(let on):
            setClause = "SET RECURSIVE_TRIGGERS \(onOff(on))"
        case .numericRoundAbort(let on):
            setClause = "SET NUMERIC_ROUNDABORT \(onOff(on))"
        case .dateCorrelationOptimization(let on):
            setClause = "SET DATE_CORRELATION_OPTIMIZATION \(onOff(on))"
        case .cursorCloseOnCommit(let on):
            setClause = "SET CURSOR_CLOSE_ON_COMMIT \(onOff(on))"
        case .cursorDefaultLocal(let local):
            setClause = "SET CURSOR_DEFAULT \(local ? "LOCAL" : "GLOBAL")"
        case .containment(let option):
            setClause = "SET CONTAINMENT = \(option.rawValue)"
        case .defaultFulltextLanguage(let lcid):
            setClause = "SET DEFAULT_FULLTEXT_LANGUAGE = \(lcid)"
        case .defaultLanguage(let lcid):
            setClause = "SET DEFAULT_LANGUAGE = \(lcid)"
        case .nestedTriggers(let on):
            setClause = "SET NESTED_TRIGGERS = \(onOff(on))"
        case .transformNoiseWords(let on):
            setClause = "SET TRANSFORM_NOISE_WORDS = \(onOff(on))"
        case .twoDigitYearCutoff(let year):
            setClause = "SET TWO_DIGIT_YEAR_CUTOFF = \(year)"
        case .restrictAccess(let access):
            setClause = "SET \(access.rawValue) WITH ROLLBACK IMMEDIATE"
        case .databaseState(let state):
            setClause = "SET \(state.rawValue)"
        }

        let result = try await client.execute("ALTER DATABASE \(escaped) \(setClause)")
        return result.messages
    }
}
