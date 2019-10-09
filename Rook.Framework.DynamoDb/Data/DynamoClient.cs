using System;
using Amazon.DynamoDBv2;
using Linq2DynamoDb.DataContext;
using Rook.Framework.Core.Common;

namespace Rook.Framework.DynamoDb.Data
{
    public class DynamoClient : IDynamoClient
    {
        private static IAmazonDynamoDB _dynamoClient;
        private IConfigurationManager _configurationManager;
        private static DataContext _context;

        public DynamoClient(IConfigurationManager configurationManager)
        {
            _configurationManager = configurationManager;
            _dynamoClient = new AmazonDynamoDBClient(
                _configurationManager.Get<string>("AWSAccessKey"),
                _configurationManager.Get<string>("AWSSecretKey"));
            _context = new DataContext(_dynamoClient,String.Empty);
        }
        
        public void Create(string connectionString = null)
        {
            throw new NotImplementedException();
        }

        public DataContext GetDatabase()
        {
            return _context;
        }
    }
}