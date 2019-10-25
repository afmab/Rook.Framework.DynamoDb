using System;
using Newtonsoft.Json;

namespace Rook.Framework.DynamoDb.Data
{


    public abstract class DataEntity 
    {
        protected DataEntity()
        {
            Id = Guid.NewGuid();
            HashKey = Guid.NewGuid().ToString();
        }

        [JsonConverter(typeof(GuidConverter))]
        public object Id { get; set; }
        
        [JsonConverter(typeof(GuidConverter))]
        public object HashKey { get; set; }
        
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

    
    public class PrimaiveConverter : JsonConverter
    {
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            throw new NotImplementedException();
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            return serializer.Deserialize<Amazon.DynamoDBv2.DocumentModel.Primitive>(reader);
        }

        public override bool CanConvert(Type objectType)
        {
            throw new NotImplementedException();
        }
    }
}