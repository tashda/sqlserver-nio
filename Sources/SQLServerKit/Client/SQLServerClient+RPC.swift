import NIO

extension SQLServerClient {
    public func call(
        procedure name: String,
        parameters: [SQLServerConnection.ProcedureParameter] = [],
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<SQLServerExecutionResult> {
        let loop = eventLoop ?? eventLoopGroup.next()
        return withConnection(on: loop) { connection in
            connection.call(procedure: name, parameters: parameters)
        }
    }
}
