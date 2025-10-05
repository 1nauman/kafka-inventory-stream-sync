using Wms.Simulator;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<WmsWorker>();

var host = builder.Build();
await host.RunAsync();