using System.Text;
using Newtonsoft.Json;

namespace ServiceBusMessageOrdering
{
    public static class JsonSerializer
    {
        public static byte[] Serialize<T>(T item) =>
        Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(item));
        
        public static T Deserialize<T>(byte[] item) =>
            JsonConvert.DeserializeObject<T>(Encoding.UTF8.GetString(item));
    }
}