using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Rook.Framework.DynamoDb.Data
{
    public interface IDynamoStore
    {
        T Get<T>(object id) where T : DataEntity;
        IEnumerable<T> Get<T>(Expression<Func<T, bool>> filter) where T : DataEntity;
        IList<T> GetList<T>(Expression<Func<T, bool>> filter) where T : DataEntity;
        void Remove<T>(object id) where T : DataEntity;
        void Put<T>(T entityToStore) where T : DataEntity;
        void Put<T>(T entityToStore, Expression<Func<T, bool>> filter) where T : DataEntity;
        bool Ping();
    }
}