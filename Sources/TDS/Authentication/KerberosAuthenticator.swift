import Foundation
import GSS
import Logging

enum KerberosError: Error {
    case invalidPasswordEncoding
    case invalidServerToken
    case nameCreation(String)
    case credentialCreation(String)
    case handshakeFailure(String)
}

extension KerberosError: LocalizedError {
    var errorDescription: String? {
        switch self {
        case .invalidPasswordEncoding:
            return "Kerberos password could not be encoded as UTF-8"
        case .invalidServerToken:
            return "Kerberos server token was invalid"
        case .nameCreation(let message):
            return "Kerberos principal creation failed: \(message)"
        case .credentialCreation(let message):
            return "Kerberos credential acquisition failed: \(message)"
        case .handshakeFailure(let message):
            return "Kerberos authentication failed: \(message)"
        }
    }
}

final class KerberosAuthenticator {
    private let logger: Logger
    private var context: gss_ctx_id_t?
    private var credential: gss_cred_id_t?
    private var targetName: gss_name_t?
    private var isComplete: Bool = false

    init(username: String, password: String, domain: String?, server: String, port: Int, logger: Logger) throws {
        self.logger = logger

        let principal: String
        if let domain, !domain.isEmpty {
            principal = "\(username)@\(domain.uppercased())"
        } else {
            principal = username
        }

        var nameError: Unmanaged<CFError>?
        guard let userName = GSSCreateName(principal as CFTypeRef, GSSUsernameNameType, &nameError) else {
            throw KerberosError.nameCreation(KerberosAuthenticator.describe(cfError: nameError?.takeRetainedValue()))
        }
        defer { GSSReleaseName(userName) }

        guard let passwordData = password.data(using: .utf8) else {
            throw KerberosError.invalidPasswordEncoding
        }

        var credentialError: Unmanaged<CFError>?
        guard let credential = GSSCreateCredentialFromPassword(nil, userName, passwordData as CFData, GSS_KRB5_MECHANISM, &credentialError) else {
            throw KerberosError.credentialCreation(KerberosAuthenticator.describe(cfError: credentialError?.takeRetainedValue()))
        }
        self.credential = credential

        let servicePrincipal: String
        if port != 0 {
            servicePrincipal = "MSSQLSvc/\(server):\(port)"
        } else {
            servicePrincipal = "MSSQLSvc/\(server)"
        }

        var serviceError: Unmanaged<CFError>?
        guard let serviceName = GSSCreateName(servicePrincipal as CFTypeRef, GSSServiceNameType, &serviceError) else {
            throw KerberosError.nameCreation(KerberosAuthenticator.describe(cfError: serviceError?.takeRetainedValue()))
        }
        self.targetName = serviceName
        self.context = GSS_C_NO_CONTEXT
    }

    deinit {
        var minorStatus: OM_uint32 = 0
        if var context = context {
            gss_delete_sec_context(&minorStatus, &context, nil)
        }
        if let credential {
            GSSReleaseCredential(credential)
        }
        if let targetName {
            GSSReleaseName(targetName)
        }
    }

    func initialToken() throws -> Data {
        guard let data = try generateToken(input: nil) else {
            throw KerberosError.handshakeFailure("Kerberos produced no initial token")
        }
        return data
    }

    func continueAuthentication(serverToken: Data) throws -> (Data?, Bool) {
        let token = try generateToken(input: serverToken)
        return (token, isComplete)
    }

    private func generateToken(input: Data?) throws -> Data? {
        guard let targetName else {
            throw KerberosError.handshakeFailure("Missing target service name")
        }

        var minorStatus: OM_uint32 = 0
        var outputBuffer = gss_buffer_desc()
        var mechType: gss_OID? = nil
        var retFlags: OM_uint32 = 0
        var timeRec: OM_uint32 = 0

        let flags = OM_uint32(GSS_C_MUTUAL_FLAG | GSS_C_SEQUENCE_FLAG)

        let majorStatus: OM_uint32
        if let input, !input.isEmpty {
            majorStatus = input.withUnsafeBytes { ptr -> OM_uint32 in
                guard let baseAddress = ptr.baseAddress else {
                    return GSS_S_FAILURE
                }
                var inputBuffer = gss_buffer_desc(length: OM_uint32(input.count), value: UnsafeMutableRawPointer(mutating: baseAddress))
                return gss_init_sec_context(
                    &minorStatus,
                    credential,
                    &context,
                    targetName,
                    GSS_KRB5_MECHANISM,
                    flags,
                    0,
                    nil,
                    &inputBuffer,
                    &mechType,
                    &outputBuffer,
                    &retFlags,
                    &timeRec
                )
            }
        } else {
            majorStatus = gss_init_sec_context(
                &minorStatus,
                credential,
                &context,
                targetName,
                GSS_KRB5_MECHANISM,
                flags,
                0,
                nil,
                nil,
                &mechType,
                &outputBuffer,
                &retFlags,
                &timeRec
            )
        }

        if majorStatus != GSS_S_COMPLETE && majorStatus != GSS_S_CONTINUE_NEEDED {
            throw KerberosError.handshakeFailure(KerberosAuthenticator.describeGSSStatus(major: majorStatus, minor: minorStatus))
        }

        isComplete = (majorStatus == GSS_S_COMPLETE)

        let data = KerberosAuthenticator.extractData(from: outputBuffer)
        gss_release_buffer(&minorStatus, &outputBuffer)
        return data
    }

    private static func extractData(from buffer: gss_buffer_desc) -> Data? {
        guard buffer.length > 0, let baseAddress = buffer.value else {
            return nil
        }
        return Data(bytes: baseAddress, count: Int(buffer.length))
    }

    private static func describe(cfError: CFError?) -> String {
        guard let cfError else { return "Unknown" }
        return CFErrorCopyDescription(cfError) as String
    }

    private static func describeGSSStatus(major: OM_uint32, minor: OM_uint32) -> String {
        func message(for status: OM_uint32, type: Int32) -> String {
            var status = status
            var minor: OM_uint32 = 0
            var messageContext: OM_uint32 = 0
            var buffer = gss_buffer_desc()
            defer { gss_release_buffer(&minor, &buffer) }
            var messages: [String] = []

            repeat {
                let code = gss_display_status(&minor, status, OM_uint32(type), nil, &messageContext, &buffer)
                guard code == GSS_S_COMPLETE else { break }
                if let pointer = buffer.value {
                    let data = Data(bytes: pointer, count: Int(buffer.length))
                    if let string = String(data: data, encoding: .utf8) {
                        messages.append(string)
                    }
                }
            } while messageContext != 0

            return messages.joined(separator: " ")
        }

        let majorMessage = message(for: major, type: GSS_C_GSS_CODE)
        let minorMessage = message(for: minor, type: GSS_C_MECH_CODE)
        return [majorMessage, minorMessage].filter { !$0.isEmpty }.joined(separator: " - ")
    }
}
