using System.Text;
using System.Text.Json;
using Confluent.Kafka;

namespace StreamSync.Infrastructure;

/// <summary>
    /// Generic implementation of Confluent.Kafka's ISerializer interface using System.Text.Json.
    /// This converts a C# object (T) into a byte array for transport over Kafka.
    /// </summary>
    /// <typeparam name="T">The type of the object to serialize (e.g., SalesTransactionEvent).</typeparam>
    public class KafkaJsonSerializer<T> : ISerializer<T> where T : class
    {
        public byte[]? Serialize(T? data, SerializationContext context)
        {
            if (data is null)
            {
                // Note: Returning null bytes for null data is expected by the Confluent client
                return null;
            }

            try
            {
                // Serialize the object to a JSON string, and then convert the string to a byte array.
                return Encoding.UTF8.GetBytes(JsonSerializer.Serialize(data));
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Serialization error for type {typeof(T).Name}: {ex.Message}");
                // In a production system, you might want to log this and handle it more gracefully
                throw;
            }
        }
    }

    /// <summary>
    /// Generic implementation of Confluent.Kafka's IDeserializer interface using System.Text.Json.
    /// This converts the byte array from Kafka back into a C# object (T).
    /// </summary>
    /// <typeparam name="T">The expected type of the object after deserialization.</typeparam>
    public class KafkaJsonDeserializer<T> : IDeserializer<T> where T : class
    {
        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull)
            {
                return null!;
            }

            try
            {
                // Convert the byte array back to a string and then deserialize the JSON string to the object.
                var jsonString = Encoding.UTF8.GetString(data.ToArray());
                var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
                return JsonSerializer.Deserialize<T>(jsonString, options);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Deserialization error for type {typeof(T).Name}: {ex.Message}");
                // In a production system, a common pattern is to use a dead letter queue (DLQ) here
                return null;
            }
        }
    }