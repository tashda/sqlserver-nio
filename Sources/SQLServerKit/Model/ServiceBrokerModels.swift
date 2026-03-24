import Foundation

// MARK: - Service Broker Models

/// A Service Broker message type.
public struct ServiceBrokerMessageType: Sendable, Equatable, Identifiable {
    public var id: String { name }
    public let name: String
    /// Validation mode: NONE, EMPTY, WELL_FORMED_XML, VALID_XML.
    public let validation: String
    public let isSystemObject: Bool

    public init(name: String, validation: String, isSystemObject: Bool) {
        self.name = name
        self.validation = validation
        self.isSystemObject = isSystemObject
    }
}

/// A Service Broker contract.
public struct ServiceBrokerContract: Sendable, Equatable, Identifiable {
    public var id: String { name }
    public let name: String
    public let isSystemObject: Bool

    public init(name: String, isSystemObject: Bool) {
        self.name = name
        self.isSystemObject = isSystemObject
    }
}

/// A Service Broker queue.
public struct ServiceBrokerQueue: Sendable, Equatable, Identifiable {
    public var id: String { "\(schema).\(name)" }
    public let schema: String
    public let name: String
    public let isActivationEnabled: Bool
    public let activationProcedure: String?
    public let maxQueueReaders: Int
    public let isReceiveEnabled: Bool
    public let isRetentionEnabled: Bool
    public let isEnqueueEnabled: Bool

    public init(
        schema: String,
        name: String,
        isActivationEnabled: Bool,
        activationProcedure: String?,
        maxQueueReaders: Int,
        isReceiveEnabled: Bool,
        isRetentionEnabled: Bool,
        isEnqueueEnabled: Bool
    ) {
        self.schema = schema
        self.name = name
        self.isActivationEnabled = isActivationEnabled
        self.activationProcedure = activationProcedure
        self.maxQueueReaders = maxQueueReaders
        self.isReceiveEnabled = isReceiveEnabled
        self.isRetentionEnabled = isRetentionEnabled
        self.isEnqueueEnabled = isEnqueueEnabled
    }
}

/// A Service Broker service.
public struct ServiceBrokerService: Sendable, Equatable, Identifiable {
    public var id: String { name }
    public let name: String
    public let queueName: String
    public let isSystemObject: Bool

    public init(name: String, queueName: String, isSystemObject: Bool) {
        self.name = name
        self.queueName = queueName
        self.isSystemObject = isSystemObject
    }
}

/// A Service Broker route.
public struct ServiceBrokerRoute: Sendable, Equatable, Identifiable {
    public var id: String { name }
    public let name: String
    public let address: String?
    public let brokerInstance: String?
    public let lifetime: Int?
    public let mirrorAddress: String?

    public init(name: String, address: String?, brokerInstance: String?, lifetime: Int?, mirrorAddress: String?) {
        self.name = name
        self.address = address
        self.brokerInstance = brokerInstance
        self.lifetime = lifetime
        self.mirrorAddress = mirrorAddress
    }
}

/// A Service Broker remote service binding.
public struct ServiceBrokerRemoteBinding: Sendable, Equatable, Identifiable {
    public var id: String { name }
    public let name: String
    public let serviceName: String
    public let principalName: String?
    public let isAnonymous: Bool

    public init(name: String, serviceName: String, principalName: String?, isAnonymous: Bool) {
        self.name = name
        self.serviceName = serviceName
        self.principalName = principalName
        self.isAnonymous = isAnonymous
    }
}

// MARK: - Creation Types

/// Validation mode for a Service Broker message type.
public enum MessageTypeValidation: Sendable, Equatable {
    case none
    case empty
    case wellFormedXML
    case validXML(schemaCollection: String)

    public var sqlClause: String {
        switch self {
        case .none: return "NONE"
        case .empty: return "EMPTY"
        case .wellFormedXML: return "WELL_FORMED_XML"
        case .validXML(let sc): return "VALID_XML WITH SCHEMA COLLECTION [\(sc.replacingOccurrences(of: "]", with: "]]"))]"
        }
    }
}

/// Direction constraint for a contract message usage.
public enum ContractSentBy: String, Sendable, Equatable {
    case initiator = "INITIATOR"
    case target = "TARGET"
    case any = "ANY"
}

/// Options for creating a Service Broker queue.
public struct QueueCreationOptions: Sendable {
    public var status: Bool
    public var retention: Bool
    public var activationEnabled: Bool
    public var activationProcedure: String?
    public var maxQueueReaders: Int
    public var executeAs: String?
    public var poisonMessageHandling: Bool

    public init(
        status: Bool = true,
        retention: Bool = false,
        activationEnabled: Bool = false,
        activationProcedure: String? = nil,
        maxQueueReaders: Int = 1,
        executeAs: String? = nil,
        poisonMessageHandling: Bool = true
    ) {
        self.status = status
        self.retention = retention
        self.activationEnabled = activationEnabled
        self.activationProcedure = activationProcedure
        self.maxQueueReaders = maxQueueReaders
        self.executeAs = executeAs
        self.poisonMessageHandling = poisonMessageHandling
    }

    public static let defaults = QueueCreationOptions()
}
