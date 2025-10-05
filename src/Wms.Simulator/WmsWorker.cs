using Confluent.Kafka;
using StreamSync.Contracts;
using StreamSync.Infrastructure;

namespace Wms.Simulator;

/// <summary>
/// This Worker Service simulates the Warehouse Management System (WMS)
/// that periodically publishes full inventory snapshots.
/// </summary>
public class WmsWorker : BackgroundService
{
    private readonly ILogger<WmsWorker> _logger;
    private readonly IProducer<string, InventoryUpdateEvent> _producer;
    private readonly IConfiguration _configuration;
    private readonly string _topicName;
    private readonly TimeSpan _simulationInterval;

    // A dictionary to track the simulated current stock and allow it to fluctuate slightly
    private readonly Dictionary<string, int> _simulatedStock = new()
    {
        { "PRODUCT-A101", 500 },
        { "PRODUCT-B202", 750 },
        { "PRODUCT-C303", 200 },
        { "PRODUCT-D404", 1000 }
    };

    public WmsWorker(ILogger<WmsWorker> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _topicName = configuration.GetValue<string>("Kafka:TopicName") ?? "inventory-snapshots";
        var intervalSeconds = configuration.GetValue<int>("Kafka:SimulationIntervalSeconds");
        _simulationInterval = TimeSpan.FromSeconds(intervalSeconds > 0 ? intervalSeconds : 5);

        // Configure the Producer. Note: Idempotence is usually less critical for snapshot streams.
        var producerConfig = configuration.GetSection("Kafka").Get<ProducerConfig>();

        // Use the Infrastructure serializer
        _producer = new ProducerBuilder<string, InventoryUpdateEvent>(producerConfig)
            .SetValueSerializer(new KafkaJsonSerializer<InventoryUpdateEvent>())
            .Build();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("WMS Snapshot Simulator starting. Publishing every {Interval} seconds.",
            _simulationInterval.TotalSeconds);

        // Use a high-frequency timer to publish snapshots
        using var timer = new PeriodicTimer(_simulationInterval);

        while (!stoppingToken.IsCancellationRequested && await timer.WaitForNextTickAsync(stoppingToken))
        {
            await PublishInventorySnapshots();
        }

        // Ensure the producer flushes any remaining messages before the service stops
        _producer.Flush(TimeSpan.FromSeconds(10));
    }

    private async Task PublishInventorySnapshots()
    {
        var random = new Random();

        foreach (var (productId, stock) in _simulatedStock)
        {
            // Simulate a slight, random change in stock
            var newStock = stock + random.Next(-5, 6);
            _simulatedStock[productId] = newStock; // Update internal state

            var eventMessage = new InventoryUpdateEvent(
                Id: Guid.NewGuid().ToString(),
                Timestamp: DateTimeOffset.UtcNow,
                ProductId: productId,
                CurrentStock: newStock
            );

            var message = new Message<string, InventoryUpdateEvent>
            {
                // KAFKA POWER: Use ProductId as the Message Key. 
                // This ensures the Inventory Processor reads the snapshots in the correct order per product.
                Key = eventMessage.Id,
                Value = eventMessage
            };

            try
            {
                var result = await _producer.ProduceAsync(_topicName, message);
                _logger.LogInformation(
                    "WMS Snapshot published: ProductId '{Key}', Stock {Stock}, Partition {Partition}, Offset {Offset}",
                    result.Message.Key,
                    result.Message.Value.CurrentStock,
                    result.Partition.Value,
                    result.Offset.Value);
            }
            catch (Exception exception)
            {
                _logger.LogError(exception, "Failed to publish WMS snapshot for product {ProductId}", productId);
            }
        }
    }
}