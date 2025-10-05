namespace StreamSync.Contracts;

// --- Stream 2: The Fast, Transactional Stream (Sales API) ---
/// <summary>
/// Represents a single completed sale or order. This event is used to instantly decrement 
/// the inventory state in the stream processor.
/// </summary>
public record SalesTransactionEvent(
    string Id,
    DateTimeOffset Timestamp,
    string OrderId,
    string ProductId,
    int QuantitySold,
    decimal SaleAmount
) : KafkaEvent(Id, Timestamp);