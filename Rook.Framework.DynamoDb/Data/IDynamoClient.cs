using Amazon.DynamoDBv2;
using Linq2DynamoDb.DataContext;

namespace Rook.Framework.DynamoDb.Data
{
    public interface IDynamoClient
    {
        void Create(string connectionString);
        DataContext GetDatabase();

    }
}