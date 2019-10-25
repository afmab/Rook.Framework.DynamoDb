using System;
using Newtonsoft.Json;

namespace Rook.Framework.DynamoDb.Data
{


    public abstract class DataEntity 
    {
        protected DataEntity()
        {
            Id = Guid.NewGuid().ToString();
            HashKey = "NotAGuid" + Guid.NewGuid().ToString().Replace('-','Z');
        }

        //[JsonConverter(typeof(GuidConverter))]
        public object Id { get; set; }
        
        public string HashKey { get; set; }
        
        [JsonIgnore]
        public DateTime ExpiresAt { get; set; } = DateTime.UtcNow.AddMonths(18);

        [JsonIgnore]
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    }
    public class GuidConverter : JsonConverter
    {
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            serializer.Serialize(writer, value);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            return serializer.Deserialize<Guid>(reader);
        }

        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(Guid);
        }
    }
}