import Foundation
import NIO
import NIOCore
import NIOPosix
import NIOSSL
import SQLServerTDS
import Logging

extension SQLServerConnection {
    internal static func resolveSocketAddresses(
        hostname: String,
        port: Int,
        transparentResolution: Bool,
        on eventLoop: EventLoop
    ) -> EventLoopFuture<[SocketAddress]> {
        let promise = eventLoop.makePromise(of: [SocketAddress].self)
        // Resolve off the event loop to avoid blocking it
        DispatchQueue.global().async {
            do {
                // Try parsing as an IP literal first (no DNS needed)
                let address = try SocketAddress.makeAddressResolvingHost(hostname, port: port)
                promise.succeed([address])
            } catch {
                promise.succeed([])
            }
        }
        return promise.futureResult
    }

    internal static func establishTDSConnection(
        addresses: [SocketAddress],
        tlsConfiguration: TLSConfiguration?,
        serverHostname: String?,
        connectTimeout: TimeAmount,
        on eventLoop: EventLoop,
        logger: Logger
    ) -> EventLoopFuture<TDSConnection> {
        @Sendable
        func attempt(_ remaining: [SocketAddress]) -> EventLoopFuture<TDSConnection> {
            guard let next = remaining.first else {
                return eventLoop.makeFailedFuture(SQLServerError.transient(NSError(domain: "SQLServerConnection", code: 4, userInfo: [NSLocalizedDescriptionKey: "No more addresses to try"])))
            }
            return TDSConnection.connect(
                to: next,
                tlsConfiguration: tlsConfiguration,
                serverHostname: serverHostname,
                connectTimeout: connectTimeout,
                on: eventLoop
            ).flatMapError { error in
                let rest = Array(remaining.dropFirst())
                if rest.isEmpty { return eventLoop.makeFailedFuture(error) }
                return attempt(rest)
            }
        }
        return attempt(addresses)
    }
}
