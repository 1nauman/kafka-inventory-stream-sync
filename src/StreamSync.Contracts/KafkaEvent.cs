namespace StreamSync.Contracts;

/// <summary>
/// Base event record to ensure all Kafka messages have core properties like ID and Timestamp.
/// Using 'record' provides immutable, value-based contracts, which is ideal for events.
/// </summary>
public abstract record KafkaEvent(string Id, DateTimeOffset Timestamp);