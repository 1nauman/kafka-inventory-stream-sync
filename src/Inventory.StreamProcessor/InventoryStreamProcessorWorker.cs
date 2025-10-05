using System.Collections.Concurrent;
using Confluent.Kafka;
using StreamSync.Contracts;
using StreamSync.Infrastructure;

namespace Inventory.StreamProcessor;

/// <summary>
/// This is the core stream processing worker. It consumes two streams: 
/// 1. 'inventory-snapshots' (slow, full truth)
/// 2. 'sales-events' (fast, incremental updates)
/// It maintains a state (ConcurrentDictionary) and publishes the derived 'net-available-stock' stream.
/// </summary>
public class InventoryStreamProcessorWorker : BackgroundService
{
    private readonly ILogger<InventoryStreamProcessorWorker> _logger;
    private readonly IConfiguration _configuration;

    // KAFKA POWER: ConcurrentDictionary acts as our local, durable 'KTable' or state store.
    // Key: ProductId (string), Value: Current Available Stock (int)
    private readonly ConcurrentDictionary<string, int> _inventoryState = new();

    // Dedicated producer for publishing the derived output stream
    private IProducer<string, NetAvailableStockEvent> _outputProducer;

    public InventoryStreamProcessorWorker(ILogger<InventoryStreamProcessorWorker> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Inventory Stream Processor starting.");

        var kafkaConfig = _configuration.GetSection("Kafka");


        var adminClient = new AdminClientBuilder(kafkaConfig.Get<AdminClientConfig>() ??
                                                        throw new InvalidOperationException(
                                                            "Kafka configuration section is missing."))
            .Build();
        
        adminClient.GetMetadata(TimeSpan.FromSeconds(10));
        
        var producerConfig = kafkaConfig.Get<ProducerConfig>() ?? new ProducerConfig();

        // Initialize the producer for the output stream
        _outputProducer = new ProducerBuilder<string, NetAvailableStockEvent>(producerConfig)
            .SetValueSerializer(new KafkaJsonSerializer<NetAvailableStockEvent>())
            .Build();

        // Launch two concurrent long-running tasks: one for each input stream.
        return Task.WhenAll(
            Task.Run(() => ConsumeSalesStream(stoppingToken), stoppingToken),
            Task.Run(() => ConsumeSnapshotStream(stoppingToken), stoppingToken)
        );
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Inventory Stream Processor stopping.");
        _outputProducer?.Dispose();
        await base.StopAsync(cancellationToken);
    }

    /// <summary>
    /// Consumer for the slow, source-of-truth 'inventory-snapshots' stream.
    /// This updates the base stock level.
    /// </summary>
    private async Task ConsumeSnapshotStream(CancellationToken stoppingToken)
    {
        var consumerConfig = _configuration.GetSection("Kafka").Get<ConsumerConfig>() ?? new ConsumerConfig();
        consumerConfig.GroupId = _configuration.GetValue<string>("Kafka:ConsumerGroupId");
        consumerConfig.AutoOffsetReset = AutoOffsetReset.Earliest; // Read from start to rebuild state
        var topicName = _configuration.GetValue<string>("Kafka:SnapshotTopicName") ?? "inventory-snapshots";

        using var consumer = new ConsumerBuilder<string, InventoryUpdateEvent>(consumerConfig)
            .SetKeyDeserializer(Deserializers.Utf8)
            .SetValueDeserializer(new KafkaJsonDeserializer<InventoryUpdateEvent>())
            .SetErrorHandler((_, e) => _logger.LogError($"Snapshot Consumer Error: {e.Reason}"))
            .Build();

        consumer.Subscribe(topicName);
        _logger.LogInformation("Snapshot Consumer subscribed to topic: {Topic}", topicName);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var consumeResult = consumer.Consume(stoppingToken);
                var eventData = consumeResult.Message.Value;

                // KAFKA POWER: This stream updates the base state (KTable logic)
                _inventoryState.AddOrUpdate(
                    eventData.ProductId,
                    eventData.CurrentStock,
                    (key, existingValue) => eventData.CurrentStock // Always overwrite with the latest snapshot
                );

                _logger.LogWarning("SNAPSHOT RECEIVED: Product {Id} Stock updated to {Stock}.",
                    eventData.ProductId, eventData.CurrentStock);

                // Publish the derived net stock immediately after a snapshot update
                await PublishNetAvailableStock(eventData.ProductId);

                consumer.Commit(consumeResult);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (ConsumeException e)
            {
                _logger.LogError(e, "Snapshot Consume Error: {Reason}", e.Error.Reason);
            }
        }
    }

    /// <summary>
    /// Consumer for the fast, transactional 'sales-events' stream.
    /// This applies incremental changes to the state.
    /// </summary>
    private async Task ConsumeSalesStream(CancellationToken stoppingToken)
    {
        var consumerConfig = _configuration.GetSection("Kafka").Get<ConsumerConfig>() ?? new ConsumerConfig();
        consumerConfig.GroupId = _configuration.GetValue<string>("Kafka:ConsumerGroupId");
        consumerConfig.AutoOffsetReset = AutoOffsetReset.Latest; // Only interested in new sales
        var topicName = _configuration.GetValue<string>("Kafka:SalesTopicName") ?? "sales-events";

        using var consumer = new ConsumerBuilder<string, SalesTransactionEvent>(consumerConfig)
            .SetKeyDeserializer(Deserializers.Utf8)
            .SetValueDeserializer(new KafkaJsonDeserializer<SalesTransactionEvent>())
            .SetErrorHandler((_, e) => _logger.LogError($"Sales Consumer Error: {e.Reason}"))
            .Build();

        consumer.Subscribe(topicName);
        _logger.LogInformation("Sales Consumer subscribed to topic: {Topic}", topicName);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var consumeResult = consumer.Consume(stoppingToken);
                var eventData = consumeResult.Message.Value;

                // KAFKA POWER: Because ProductId is the Key, all sales for one product 
                // hit this consumer in ORDER, guaranteeing the math is correct.
                _inventoryState.AddOrUpdate(
                    eventData.ProductId,
                    // If the product doesn't exist (no snapshot yet), assume 0 and apply the sale.
                    // This creates a state deficiency, which the next snapshot will correct.
                    -eventData.QuantitySold,
                    (key, existingStock) => existingStock - eventData.QuantitySold
                );

                _logger.LogInformation("SALE PROCESSED: Product {Id}, Sold {Qty}. New Internal Stock: {Stock}.",
                    eventData.ProductId, eventData.QuantitySold, _inventoryState[eventData.ProductId]);

                // Publish the derived net stock immediately after the sale
                await PublishNetAvailableStock(eventData.ProductId);

                consumer.Commit(consumeResult);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (ConsumeException e)
            {
                _logger.LogError(e, "Sales Consume Error: {Reason}", e.Error.Reason);
            }
        }
    }

    /// <summary>
    /// Publishes the calculated, derived net available stock to the output topic.
    /// </summary>
    private async Task PublishNetAvailableStock(string productId)
    {
        if (!_inventoryState.TryGetValue(productId, out int availableStock))
        {
            // Should not happen if snapshots or sales have occurred.
            _logger.LogWarning("Cannot publish net stock: ProductId {Id} not found in state.", productId);
            return;
        }

        var outputEvent = new NetAvailableStockEvent(
            Id: Guid.NewGuid().ToString(),
            Timestamp: DateTimeOffset.UtcNow,
            ProductId: productId,
            AvailableStock: availableStock
        );

        var message = new Message<string, NetAvailableStockEvent>
        {
            Key = productId,
            Value = outputEvent
        };

        var topicName = _configuration.GetValue<string>("Kafka:OutputTopicName") ?? "net-available-stock";

        try
        {
            var deliveryResult = await _outputProducer.ProduceAsync(topicName, message);
            _logger.LogCritical("--> NET STOCK PUBLISHED: Product {Id}, Stock {Stock}. Partition {Partition}.",
                productId, availableStock, deliveryResult.Partition.Value);
        }
        catch (ProduceException<string, NetAvailableStockEvent> e)
        {
            _logger.LogError(e, "Failed to publish net stock for product {ProductId}", productId);
        }
    }
}