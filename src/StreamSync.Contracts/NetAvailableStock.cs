namespace StreamSync.Contracts;

// --- Stream 3: The Derived Output Stream (Real-Time Availability) ---
/// <summary>
/// Represents the real-time calculated net available stock after processing
/// both sales and WMS snapshot events.
/// </summary>
public record NetAvailableStockEvent(
    string Id,
    DateTimeOffset Timestamp,
    string ProductId,
    int AvailableStock // The synchronized, real-time calculated stock
);