import Foundation
import Logging

enum KerberosError: Error {
    case unsupported
}

final class KerberosAuthenticator {
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
