import NIO

/// Internal user events used by the TDS pipeline.
enum TDSUserEvent {
    case attention
    case failCurrentRequestTimeout
}
