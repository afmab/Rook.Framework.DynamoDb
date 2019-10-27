using System;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;

namespace Rook.Framework.DynamoDb.Helpers
{
    public class TimeSpanConverter : IPropertyConverter
    {
        public DynamoDBEntry ToEntry(object value)
        {
            TimeSpan time = (TimeSpan) value;
            var timeValues = new PrimitiveList(DynamoDBEntryType.Numeric);
            timeValues.Add(time.Days);
            timeValues.Add(time.Hours);
            timeValues.Add(time.Minutes);
            timeValues.Add(time.Seconds);
            timeValues.Add(time.Milliseconds);
            return timeValues;
        }

        public object FromEntry(DynamoDBEntry entry)
        {
            var timeValues = entry.AsPrimitiveList();
            return new TimeSpan(timeValues[0].AsInt(), timeValues[1].AsInt(), timeValues[2].AsInt(),
                timeValues[3].AsInt(), timeValues[4].AsInt());

        }
    }
}