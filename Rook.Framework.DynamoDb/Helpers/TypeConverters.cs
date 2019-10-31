using System;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;

namespace Rook.Framework.DynamoDb.Helpers
{

    public class TimeSpanConverter : IPropertyConverter
    {
        public DynamoDBEntry ToEntry(object value)
        {
            return new Primitive(value.ToString());
        }

        public object FromEntry(DynamoDBEntry entry)
        {
            return TimeSpan.Parse(entry.AsPrimitive().AsString());
        }
    }

    public class NullableDateTimeConverter : IPropertyConverter
    {
        public DynamoDBEntry ToEntry(object value)
        {
            return value == null ? new Primitive("") : new Primitive(value.ToString());
        }

        public object FromEntry(DynamoDBEntry entry)
        {
            return entry == null || entry == "" ? new DateTime() : DateTime.Parse(entry.ToString());
        }
    }
}
