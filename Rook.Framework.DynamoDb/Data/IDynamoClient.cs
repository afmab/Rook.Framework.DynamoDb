using Amazon.DynamoDBv2;

namespace Rook.Framework.DynamoDb.Data
{
    public interface IDynamoClient
    {
        void Create();
        AmazonDynamoDBClient GetDatabase();
    }
}