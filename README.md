# 🛒 Real-Time Inventory Stream Synchronization (.NET & Kafka)

This project demonstrates a core Event-Driven Architecture (EDA) pattern: joining and aggregating two disparate data streams in real-time to calculate a single, consistent state—**Net Available Stock**.

The system uses three separate .NET services running concurrently against a single Apache Kafka broker running in ZooKeeper-less **KRaft mode**.

## ✨ Core Concepts

| **Service** | **Role** | **Topics Handled** | **Description** |
| :--- | :--- | :--- | :--- |
| **Wms.Simulator** | **Slow Producer** | `inventory-snapshots` | Provides the initial, full stock count (the slower stream). Uses the **AdminClient** to create all topics on startup. |
| **Sales.Api** | **Fast Producer** | `sales-events` | Simulates high-velocity sales transactions. Configured for **Idempotence** (exactly-once delivery). |
| **Inventory.StreamProcessor** | **Dual Consumer/Processor** | `net-available-stock` (Output) | Consumes both streams, maintains the current stock state, and publishes the real-time net result. |

### Partitioning for Order Guarantee

* **Key:** All messages use the **`ProductId`** as the Partition Key.
* **Guarantee:** This ensures that all events (snapshots and sales) for a single product are routed to the **same partition**. This is crucial for maintaining strict processing order and guaranteeing accurate stock calculation.

---

## 🛠️ Prerequisites

1.  **Docker:** Required to run the Kafka broker container.
2.  **.NET 8+ SDK:** Required to build and run the C# applications.

---

## 🚀 Getting Started

Follow these steps to launch the entire synchronization pipeline.

### Step 1: Start the Kafka Broker (KRaft Mode)

Launch the official Apache Kafka image in ZooKeeper-less KRaft mode using the single `docker run` command:

```bash
docker run -d -p 9092:9092 apache/kafka:4.1.0
````

> **Note:** The broker requires a moment to fully initialize. The C\# application startup includes a delay to ensure the topics are created before consumption starts.

### Step 2: Run the .NET Services

Navigate to the project root directory and use the shell's concurrent execution operator (`&`) to launch all three applications simultaneously:

```bash
# Run this command from the solution root directory (RealTimeKafkaSync)

dotnet run --project src/Sales.Api/Sales.Api.csproj & \
dotnet run --project src/Wms.Simulator/Wms.Simulator.csproj & \
dotnet run --project src/Inventory.StreamProcessor/Inventory.StreamProcessor.csproj
```

### Step 3: Verify Initialization

Watch the terminal logs. The services should start without the "Unknown topic or partition" errors.

1.  Look for `Wms.Simulator` logs confirming **topic creation**.
2.  Look for `Inventory.StreamProcessor` logs showing initial **`SNAPSHOT RECEIVED`** followed by **`--> NET STOCK PUBLISHED`**.

### Step 4: Test Real-Time Synchronization

Use a tool like `curl` (or Postman/Insomnia) to send sales requests to the API.

```bash
# Sample Sale for PRODUCT-A101
curl -X POST http://localhost:5000/sales -H "Content-Type: application/json" -d '{"productId":"PRODUCT-A101","quantity":10}'
```

Watch the **`Inventory.StreamProcessor`** logs in real-time. You should see the stock instantly decrease, demonstrating the fast stream synchronization.

-----

## 🛑 Shutting Down

1.  **Stop C\# Applications:** Use `Ctrl + C` in the terminal where you launched the services to gracefully stop all three processes.

2.  **Stop Kafka:** Stop the Docker container:

    ```bash
    docker stop $(docker ps -q --filter ancestor=apache/kafka:4.1.0)
    ```