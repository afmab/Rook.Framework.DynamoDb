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
        internal readonly ILogger Logger;

        public DynamoClient(IConfigurationManager configurationManager,ILogger logger)
        {
            Logger = logger;
            _configurationManager = configurationManager;
            var serviceUrl = _configurationManager.Get<string>("AWSServiceURL");
            AmazonDynamoDBConfig conf = new AmazonDynamoDBConfig();
            if (serviceUrl != "")
            {
                Logger.Info("Local");
                conf.ServiceURL = serviceUrl;
                
                _dynamoClient = new AmazonDynamoDBClient(
                    _configurationManager.Get<string>("AWSAccessKey"),
                    _configurationManager.Get<string>("AWSSecretKey"),conf);
            }
            else
            {
                Logger.Info("remote");
                _dynamoClient = new AmazonDynamoDBClient();
            }

            var environment = configurationManager.Get<string>("ENVIRONMENT");
            if (string.IsNullOrEmpty(environment))
                environment = "dev";
            
            _context = new DataContext(_dynamoClient,environment);
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
