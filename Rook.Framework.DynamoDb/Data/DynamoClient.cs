using System;
using Amazon;
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
            var serviceUrl = _configurationManager.Get<string>("AWSServiceURL");
            AmazonDynamoDBConfig conf = new AmazonDynamoDBConfig();
            if (serviceUrl != "")
            {
                conf.ServiceURL = serviceUrl;
                _dynamoClient = new AmazonDynamoDBClient(
                    _configurationManager.Get<string>("AWSAccessKey"),
                    _configurationManager.Get<string>("AWSSecretKey"),conf);
            }
            else
            {
                conf.RegionEndpoint = RegionEndpoint.EUWest1;
                _dynamoClient = new AmazonDynamoDBClient(
                    _configurationManager.Get<string>("AWSAccessKey"),
                    _configurationManager.Get<string>("AWSSecretKey"),conf);
            }
            
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