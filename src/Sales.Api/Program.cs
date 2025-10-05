using Confluent.Kafka;
using StreamSync.Contracts;
using StreamSync.Infrastructure;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();

// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();

// --- 1. Kafka configuration ---
var kafkaConfig = builder.Configuration.GetSection("Kafka").Get<ProducerConfig>();

// Best practice: Use a dedicated factory method to configure the producer.
builder.Services.AddSingleton<IProducer<string, SalesTransactionEvent>>(serviceProvider =>
{
    if (kafkaConfig == null)
    {
        throw new InvalidOperationException("Kafka configuration section is missing.");
    }

    kafkaConfig.EnableIdempotence = true;

    var producerBuilder = new ProducerBuilder<string, SalesTransactionEvent>(kafkaConfig)
        .SetValueSerializer(new KafkaJsonSerializer<SalesTransactionEvent>())
        .Build();

    return producerBuilder;
});
// ------------------------------

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();
app.UseAuthorization();
app.MapControllers();

// --- 2. Define the API endpoint for a new sale ---
app.MapPost("/sales", async (
    SaleRequest request,
    IProducer<string, SalesTransactionEvent> producer,
    IConfiguration config) =>
{
    if (request.QuantitySold <= 0)
    {
        return Results.BadRequest("Quantity must be greater than zero.");
    }

    if (string.IsNullOrWhiteSpace(request.ProductId))
    {
        return Results.BadRequest("ProductId is required.");
    }

    var eventMessage = new SalesTransactionEvent(
        Id: Guid.NewGuid().ToString(),
        Timestamp: DateTimeOffset.UtcNow,
        OrderId: Guid.NewGuid().ToString(),
        ProductId: request.ProductId,
        QuantitySold: request.QuantitySold,
        SaleAmount: request.SaleAmount
    );

    var message = new Message<string, SalesTransactionEvent>
    {
        Key = eventMessage.Id,
        Value = eventMessage
    };

    try
    {
        var topicName = config.GetValue<string>("Kafka:TopicName") ?? "sales-events";

        // Produce the message asynchronously
        var deliveryResult = await producer.ProduceAsync(topicName, message);

        Console.WriteLine(
            $"Produced Sale event for ProductId '{deliveryResult.Message.Key}' to topic {deliveryResult.Topic}, partition {deliveryResult.Partition}, offset {deliveryResult.Offset}");

        return Results.Created($"/sales/{eventMessage.OrderId}", eventMessage);
    }
    catch (ProduceException<string, SalesTransactionEvent> e)
    {
        Console.WriteLine($"Delivery failed: {e.Error.Reason}");
        // Note: A real-world application would implement retries and proper failure handling here.
        return Results.Problem($"Failed to produce sale event: {e.Error.Reason}", statusCode: 500);
    }
});

app.Run();

// DTO for the incoming HTTP request body
public record SaleRequest(
    string ProductId,
    int QuantitySold,
    decimal SaleAmount
);