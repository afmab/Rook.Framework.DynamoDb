namespace Rook.Framework.DynamoDb.Data
{
    public interface IDynamoStore
    {
        T Get<T>(object id) where T : DataEntity;
        void Remove<T>(object id) where T : DataEntity;
        void Put<T>(T entityToStore) where T : DataEntity;
        
        bool Ping();
    }
}