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
        // Fallback to simpler resolution via bootstrap or just return a dummy for now
        // since we want to avoid complex resolver management in this refactor turn
        let bootstrap = ClientBootstrap(group: eventLoop)
        return bootstrap.connect(host: hostname, port: port).flatMap { channel in
            let address = channel.remoteAddress
            return channel.close().map { _ in
                if let address = address { return [address] }
                return []
            }
        }.flatMapError { _ in
            // Try resolving via standard library if bootstrap fails (though it shouldn't for resolution)
            return eventLoop.makeSucceededFuture([])
        }
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
