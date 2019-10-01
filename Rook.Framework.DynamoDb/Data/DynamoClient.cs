using Amazon.DynamoDBv2;
using Rook.Framework.Core.Common;

namespace Rook.Framework.DynamoDb.Data
{
    public class DynamoClient : IDynamoClient
    {
        private AmazonDynamoDBClient _dynamoClient;
        private IConfigurationManager _configurationManager;

        public DynamoClient(IConfigurationManager configurationManager)
        {
            _configurationManager = configurationManager;
        }

        public void Create()
        {
            var databaseUri = _configurationManager.Get<string>("MongoDatabaseUri");
            var dtableName = _configurationManager.Get<string>("MongoDatabaseName");
            _dynamoClient = new AmazonDynamoDBClient();
        }
        
        public AmazonDynamoDBClient GetDatabase()
        {
            if (_dynamoClient == null) throw new AmazonDynamoDBException($"{nameof(DynamoClient)}.{nameof(Create)} must be run before calling {nameof(GetDatabase)}");

            return _dynamoClient;
        }
    }
}