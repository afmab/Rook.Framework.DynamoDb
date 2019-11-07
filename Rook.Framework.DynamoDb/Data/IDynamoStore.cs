using System;
using System.Collections.Generic;
using System.Linq;
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
        IQueryable<T> QueryableCollection<T>() where T : DataEntity;
        void Remove<T>(Expression<Func<T, bool>> filter) where T : DataEntity;
        void Update<T>(T entityToStore) where T : DataEntity;
        void RemoveEntity<T>(T entityToRemove) where T : DataEntity;
        long Count<T>() where T : DataEntity;
        long Count<T>(Expression<Func<T, bool>> expression) where T : DataEntity;
        IEnumerable<T> GetTable<T>() where T : DataEntity;
        void RefreshTableCache<T>() where T : DataEntity;
        void DropTable<T>() where T : DataEntity;


    }
}