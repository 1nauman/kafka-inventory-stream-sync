using Inventory.StreamProcessor;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<InventoryStreamProcessorWorker>();

var host = builder.Build();
host.Run();
