import Foundation

public struct SQLServerAgentPermissionReport: Sendable {
    public let isSysadmin: Bool
    public let hasAlterAnyCredential: Bool
    public let msdbRoles: [String]

    public init(isSysadmin: Bool, hasAlterAnyCredential: Bool, msdbRoles: [String]) {
        self.isSysadmin = isSysadmin
        self.hasAlterAnyCredential = hasAlterAnyCredential
        self.msdbRoles = msdbRoles
    }
}
