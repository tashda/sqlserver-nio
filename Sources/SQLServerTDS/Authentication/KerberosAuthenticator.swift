import Foundation
import Logging

#if canImport(GSS)
@preconcurrency import GSS

// GSSAPI status code constants (C macros not importable to Swift)
private let kGSS_S_COMPLETE: OM_uint32 = 0
private let kGSS_S_CONTINUE_NEEDED: OM_uint32 = 1
private let kGSS_C_GSS_CODE: Int32 = 1
private let kGSS_C_MECH_CODE: Int32 = 2
private let kGSS_C_MUTUAL_FLAG: OM_uint32 = 2
private let kGSS_C_DELEG_FLAG: OM_uint32 = 1

// Thread-safe copies of GSS extern OID descriptors.
// These are effectively constant after dyld initialization.
private nonisolated(unsafe) var gssNTUserName = __gss_c_nt_user_name_oid_desc
private nonisolated(unsafe) var gssNTHostBasedService = __gss_c_nt_hostbased_service_oid_desc
private nonisolated(unsafe) var gssKrb5Mechanism = __gss_krb5_mechanism_oid_desc
private nonisolated(unsafe) var gssSPNEGOMechanism = __gss_spnego_mechanism_oid_desc

public enum KerberosError: Error, Sendable, CustomStringConvertible {
    case unsupported
    case gssError(major: UInt32, minor: UInt32, message: String)
    case nameImportFailed(String)
    case credentialAcquisitionFailed(String)
    case contextInitFailed(String)
    case noTokenProduced

    public var description: String {
        switch self {
        case .unsupported:
            return "Kerberos authentication is not supported on this platform"
        case .gssError(let major, let minor, let message):
            return "GSS error (major=\(major), minor=\(minor)): \(message)"
        case .nameImportFailed(let detail):
            return "Failed to import GSS name: \(detail)"
        case .credentialAcquisitionFailed(let detail):
            return "Failed to acquire Kerberos credentials: \(detail)"
        case .contextInitFailed(let detail):
            return "Failed to initialize security context: \(detail)"
        case .noTokenProduced:
            return "GSSAPI produced no output token"
        }
    }
}

/// Wraps macOS GSS.framework to perform SPNEGO/Kerberos authentication
/// for SQL Server connections using the TDS SSPI authentication flow.
final class KerberosAuthenticator: @unchecked Sendable {
    private let servicePrincipalName: String
    private var context: gss_ctx_id_t?
    private var credentials: gss_cred_id_t?
    private let logger: Logger

    init(username: String, password: String, domain: String?, server: String, port: Int, logger: Logger) throws {
        self.logger = logger
        self.servicePrincipalName = "MSSQLSvc/\(server):\(port)"
        logger.debug("Kerberos SPN: \(self.servicePrincipalName)")

        if !username.isEmpty && !password.isEmpty {
            try acquireCredentials(username: username, password: password, domain: domain)
        }
    }

    deinit {
        var minorStatus: OM_uint32 = 0
        if context != nil {
            gss_delete_sec_context(&minorStatus, &context, nil)
        }
        if credentials != nil {
            gss_release_cred(&minorStatus, &credentials)
        }
    }

    func initialToken() throws -> Data {
        return try initSecurityContext(inputToken: nil)
    }

    func continueAuthentication(serverToken: Data) throws -> (Data?, Bool) {
        do {
            let responseToken = try initSecurityContext(inputToken: serverToken)
            return (responseToken, false)
        } catch KerberosError.noTokenProduced {
            return (nil, true)
        }
    }

    // MARK: - Private

    private func acquireCredentials(username: String, password: String, domain: String?) throws {
        var minorStatus: OM_uint32 = 0

        let principalString: String
        if let domain, !domain.isEmpty {
            principalString = "\(username)@\(domain.uppercased())"
        } else {
            principalString = username
        }

        var clientName: gss_name_t?
        let majorStatus = principalString.withCString { cString -> OM_uint32 in
            var nameBuffer = gss_buffer_desc(
                length: strlen(cString),
                value: UnsafeMutableRawPointer(mutating: cString)
            )
            return gss_import_name(
                &minorStatus,
                &nameBuffer,
                &gssNTUserName,
                &clientName
            )
        }

        guard majorStatus == kGSS_S_COMPLETE, let name = clientName else {
            throw KerberosError.nameImportFailed(gssStatusMessage(major: majorStatus, minor: minorStatus))
        }

        defer {
            var ms: OM_uint32 = 0
            gss_release_name(&ms, &clientName)
        }

        guard let passwordData = password.data(using: .utf8) else {
            throw KerberosError.credentialAcquisitionFailed("Password contains invalid characters")
        }
        let cfPassword = passwordData as CFData

        var error: Unmanaged<CFError>?
        let attributes: CFDictionary = [
            kGSSICPassword: cfPassword
        ] as NSDictionary

        let credMajor = gss_aapl_initial_cred(
            name,
            &gssKrb5Mechanism,
            attributes,
            &credentials,
            &error
        )

        if credMajor != kGSS_S_COMPLETE {
            let errorDesc = error?.takeRetainedValue().localizedDescription ?? "Unknown error"
            throw KerberosError.credentialAcquisitionFailed(errorDesc)
        }

        logger.debug("Kerberos credentials acquired for \(principalString)")
    }

    private func initSecurityContext(inputToken: Data?) throws -> Data {
        var minorStatus: OM_uint32 = 0

        var targetName: gss_name_t?
        let importMajor = servicePrincipalName.withCString { cString -> OM_uint32 in
            var serviceNameBuffer = gss_buffer_desc(
                length: strlen(cString),
                value: UnsafeMutableRawPointer(mutating: cString)
            )
            return gss_import_name(
                &minorStatus,
                &serviceNameBuffer,
                &gssNTHostBasedService,
                &targetName
            )
        }

        guard importMajor == kGSS_S_COMPLETE, let target = targetName else {
            throw KerberosError.nameImportFailed(gssStatusMessage(major: importMajor, minor: minorStatus))
        }

        defer {
            var ms: OM_uint32 = 0
            var name = targetName
            gss_release_name(&ms, &name)
        }

        var outputTokenBuffer = gss_buffer_desc()
        var actualMech: gss_OID?
        var retFlags: OM_uint32 = 0
        let requestFlags = kGSS_C_MUTUAL_FLAG | kGSS_C_DELEG_FLAG

        let majorStatus: OM_uint32
        if let inputToken {
            majorStatus = try inputToken.withUnsafeBytes { rawBuffer -> OM_uint32 in
                guard let baseAddress = rawBuffer.baseAddress else {
                    throw KerberosError.contextInitFailed("Server sent empty SSPI challenge token")
                }
                var inputTokenBuffer = gss_buffer_desc(
                    length: rawBuffer.count,
                    value: UnsafeMutableRawPointer(mutating: baseAddress)
                )
                return gss_init_sec_context(
                    &minorStatus,
                    credentials,
                    &context,
                    target,
                    &gssSPNEGOMechanism,
                    requestFlags,
                    0,
                    nil, // GSS_C_NO_CHANNEL_BINDINGS = nil
                    &inputTokenBuffer,
                    &actualMech,
                    &outputTokenBuffer,
                    &retFlags,
                    nil
                )
            }
        } else {
            majorStatus = gss_init_sec_context(
                &minorStatus,
                credentials,
                &context,
                target,
                &gssSPNEGOMechanism,
                requestFlags,
                0,
                nil,
                nil,
                &actualMech,
                &outputTokenBuffer,
                &retFlags,
                nil
            )
        }

        guard majorStatus == kGSS_S_COMPLETE || majorStatus == kGSS_S_CONTINUE_NEEDED else {
            throw KerberosError.contextInitFailed(gssStatusMessage(major: majorStatus, minor: minorStatus))
        }

        guard outputTokenBuffer.length > 0, let value = outputTokenBuffer.value else {
            if majorStatus == kGSS_S_COMPLETE {
                throw KerberosError.noTokenProduced
            }
            throw KerberosError.contextInitFailed("No output token produced during context initialization")
        }

        let outputData = Data(bytes: value, count: outputTokenBuffer.length)

        var ms: OM_uint32 = 0
        gss_release_buffer(&ms, &outputTokenBuffer)

        logger.debug("SPNEGO token produced (\(outputData.count) bytes), continue=\(majorStatus == kGSS_S_CONTINUE_NEEDED)")
        return outputData
    }

    private func gssStatusMessage(major: OM_uint32, minor: OM_uint32) -> String {
        var minorStatus: OM_uint32 = 0
        var msgCtx: OM_uint32 = 0
        var statusBuffer = gss_buffer_desc()
        var messages: [String] = []

        repeat {
            gss_display_status(&minorStatus, major, kGSS_C_GSS_CODE, nil, &msgCtx, &statusBuffer)
            if let value = statusBuffer.value {
                let msg = String(
                    bytesNoCopy: value,
                    length: statusBuffer.length,
                    encoding: .utf8,
                    freeWhenDone: false
                ) ?? "Unknown"
                messages.append(msg)
            }
            var ms: OM_uint32 = 0
            gss_release_buffer(&ms, &statusBuffer)
        } while msgCtx != 0

        msgCtx = 0
        repeat {
            gss_display_status(&minorStatus, minor, kGSS_C_MECH_CODE, nil, &msgCtx, &statusBuffer)
            if let value = statusBuffer.value {
                let msg = String(
                    bytesNoCopy: value,
                    length: statusBuffer.length,
                    encoding: .utf8,
                    freeWhenDone: false
                ) ?? "Unknown"
                messages.append(msg)
            }
            var ms: OM_uint32 = 0
            gss_release_buffer(&ms, &statusBuffer)
        } while msgCtx != 0

        return messages.joined(separator: "; ")
    }
}

#else

public enum KerberosError: Error, Sendable, CustomStringConvertible {
    case unsupported

    public var description: String {
        "Kerberos/GSSAPI authentication is not supported on this platform"
    }
}

final class KerberosAuthenticator: @unchecked Sendable {
    init(username: String, password: String, domain: String?, server: String, port: Int, logger: Logger) throws {
        throw KerberosError.unsupported
    }

    func initialToken() throws -> Data {
        throw KerberosError.unsupported
    }

    func continueAuthentication(serverToken: Data) throws -> (Data?, Bool) {
        throw KerberosError.unsupported
    }
}

#endif
