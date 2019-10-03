using Amazon.DynamoDBv2;
using Linq2DynamoDb.DataContext;
using Rook.Framework.Core.Common;

namespace Rook.Framework.DynamoDb.Data
{
    public class DynamoClient : IDynamoClient
    {
        private static IAmazonDynamoDB _dynamoClient;
        private IConfigurationManager _configurationManager;

        public DynamoClient() 
        {
            
        }

        static DynamoClient()
        {
            
        }

        public void Create()
        {
            var databaseUri = _configurationManager.Get<string>("MongoDatabaseUri");
            var dtableName = _configurationManager.Get<string>("MongoDatabaseName");
            _dynamoClient = new AmazonDynamoDBClient();
        }

        public AmazonDynamoDBClient GetDatabase()
        {
            throw new System.NotImplementedException();
        }

//        public AmazonDynamoDBClient GetDatabase()
//        {
//            if (_dynamoClient == null) throw new AmazonDynamoDBException($"{nameof(DynamoClient)}.{nameof(Create)} must be run before calling {nameof(GetDatabase)}");
//
//            return _dynamoClient;
//        }
    }
}