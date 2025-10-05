namespace StreamSync.Contracts;

// --- Stream 1: The Slow, Source-of-Truth Stream (WMS) ---
/// <summary>
/// Represents a periodic snapshot of a product's stock count from the Warehouse Management System (WMS).
/// This is used to build the initial 'KTable' state in the consumer.
/// </summary>
public record InventoryUpdateEvent(
    string Id,
    DateTimeOffset Timestamp,
    string ProductId,
    int CurrentStock
    ) : KafkaEvent(Id, Timestamp);