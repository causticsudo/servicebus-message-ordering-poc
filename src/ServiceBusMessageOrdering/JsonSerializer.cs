using System.Text;
using Newtonsoft.Json;

namespace ServiceBusMessageOrdering
{
    public class JsonSerializer
    {
        public byte[] Serialize<T>(T item) =>
        Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(item));
        
        public T Deserialize<T>(byte[] item) =>
            JsonConvert.DeserializeObject<T>(Encoding.UTF8.GetString(item));
    }
}