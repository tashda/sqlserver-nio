
import NIOCore

protocol TokenHandler: AnyObject {
    var promise: EventLoopPromise<Void> { get }
    var columns: [TDSTokens.ColMetadataToken.ColumnData] { get }

    func onColMetadata(_ token: TDSTokens.ColMetadataToken)
    func onRow(_ token: TDSTokens.RowToken)
    func onDone(_ token: TDSTokens.DoneToken)
    func onMessage(_ token: TDSTokens.ErrorInfoToken)
    func onReturnValue(_ token: TDSTokens.ReturnValueToken)
}

final class RequestTokenHandler: TokenHandler {
    let promise: EventLoopPromise<Void>
    let onRow: ((TDSRow) -> Void)?
    let onMetadata: (([TDSTokens.ColMetadataToken.ColumnData]) -> Void)?
    let onDone: ((TDSTokens.DoneToken) -> Void)?
    let onMessageCallback: ((TDSTokens.ErrorInfoToken, Bool) -> Void)?
    let onReturnValueCallback: ((TDSTokens.ReturnValueToken) -> Void)?
    var columns: [TDSTokens.ColMetadataToken.ColumnData] = []

    init(promise: EventLoopPromise<Void>,
         onRow: ((TDSRow) -> Void)?,
         onMetadata: (([TDSTokens.ColMetadataToken.ColumnData]) -> Void)?,
         onDone: ((TDSTokens.DoneToken) -> Void)?,
         onMessage: ((TDSTokens.ErrorInfoToken, Bool) -> Void)?,
         onReturnValue: ((TDSTokens.ReturnValueToken) -> Void)?) {
        self.promise = promise
        self.onRow = onRow
        self.onMetadata = onMetadata
        self.onDone = onDone
        self.onMessageCallback = onMessage
        self.onReturnValueCallback = onReturnValue
    }

    func onColMetadata(_ token: TDSTokens.ColMetadataToken) {
        self.columns = token.colData
        self.onMetadata?(token.colData)
    }

    func onRow(_ token: TDSTokens.RowToken) {
        // no-op
    }

    func onDone(_ token: TDSTokens.DoneToken) {
        // no-op
    }

    func onMessage(_ token: TDSTokens.ErrorInfoToken) {
        self.onMessageCallback?(token, token.type == .error)
    }

    func onReturnValue(_ token: TDSTokens.ReturnValueToken) {
        self.onReturnValueCallback?(token)
    }
}
