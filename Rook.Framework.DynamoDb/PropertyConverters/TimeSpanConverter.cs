using System;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;

namespace Rook.Framework.DynamoDb.PropertyConverters
{
    public class TimeSpanConverter : IPropertyConverter
    {
        public DynamoDBEntry ToEntry(object value)
        {
            TimeSpan time = (TimeSpan) value;
            return new Primitive(time.Ticks.ToString());
        }

        public object FromEntry(DynamoDBEntry entry)
        {
            string time = (string) entry;
            return new TimeSpan(long.Parse(time));
        }
    }
}