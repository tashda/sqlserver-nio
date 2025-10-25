import Foundation
import NIO
import Logging

public final class TDSConnection {
    let channel: Channel
    
    public var eventLoop: EventLoop {
        return self.channel.eventLoop
    }
    
    public var closeFuture: EventLoopFuture<Void> {
        return channel.closeFuture
    }
    
    public var logger: Logger

    private var didClose: Bool

    public var isClosed: Bool {
        return !self.channel.isActive
    }
    
    // Transaction state management
    private var currentTransactionDescriptor: [UInt8] = [0, 0, 0, 0, 0, 0, 0, 0] // 8 bytes like Microsoft
    private var outstandingRequestCount: UInt32 = 1
    private var isInTransaction: Bool = false
    
    init(channel: Channel, logger: Logger) {
        self.channel = channel
        self.logger = logger
        self.didClose = false
    }
    
    // Transaction state accessors
    public var transactionDescriptor: [UInt8] {
        return currentTransactionDescriptor
    }
    
    public var requestCount: UInt32 {
        return outstandingRequestCount
    }
    
    internal func updateTransactionState(descriptor: [UInt8], requestCount: UInt32) {
        self.currentTransactionDescriptor = descriptor
        self.outstandingRequestCount = requestCount
        self.isInTransaction = !descriptor.allSatisfy { $0 == 0 }
        let descriptorHex = descriptor.map { String(format: "%02x", $0) }.joined()
        logger.trace("TDS transaction state updated: descriptor=\(descriptorHex), requestCount=\(requestCount), inTransaction=\(isInTransaction)")
    }
    
    public func close() -> EventLoopFuture<Void> {
        guard !self.didClose else {
            return self.eventLoop.makeSucceededFuture(())
        }
        self.didClose = true
       
        let promise = self.eventLoop.makePromise(of: Void.self)
        self.eventLoop.execute {
            self.channel.close(mode: .all, promise: promise)
        }
        return promise.futureResult
    }
    
    deinit {
        assert(self.didClose, "TDSConnection deinitialized before being closed.")
    }
}

extension TDSConnection: @unchecked Sendable {}
