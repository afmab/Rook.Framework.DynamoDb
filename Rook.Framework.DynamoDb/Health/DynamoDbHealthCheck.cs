using System;
using Rook.Framework.Core.Common;
using Rook.Framework.Core.Health;
using Rook.Framework.DynamoDb.Data;

namespace Rook.Framework.DynamoDb.Health
{
    public class DynamoDbHealthCheck : IHealthCheck
    {
        private readonly ILogger _logger;
        private readonly IDynamoStore _dynamoStore;

        public DynamoDbHealthCheck(ILogger logger, IDynamoStore dynamoStore)
        {
            _logger = logger;
            _dynamoStore = dynamoStore;
        }
        
        public bool IsHealthy()
        {
            try
            {
                return _dynamoStore.Ping();
            }
            catch (Exception ex)
            {
                _logger.Error($"{nameof(DynamoDbHealthCheck)}.{nameof(IsHealthy)}",
                    new LogItem("Result", "Failed"),
                    new LogItem("Exception", ex.ToString));

                return false;
            }
        }
    }
}