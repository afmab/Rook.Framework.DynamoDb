using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;
using Linq2DynamoDb.DataContext;
using Linq2DynamoDb.DataContext.Caching.Redis;
using Newtonsoft.Json;
using Rook.Framework.Core.Common;
using Rook.Framework.Core.Services;
using Rook.Framework.Core.StructureMap;
using StackExchange.Redis;
using OperationType = Amazon.DynamoDBv2.OperationType;

namespace Rook.Framework.DynamoDb.Data
{
    public sealed class DynamoStore :   IStartable, IDynamoStore
    {
        private readonly DataContext _context;
        private readonly IContainerFacade _containerFacade;
        internal readonly ILogger Logger;
        private static  ConnectionMultiplexer _redisConn;
        internal static Dictionary<Type, object> TableCache { get; } = new Dictionary<Type, object>();
        public StartupPriority StartupPriority { get; } = StartupPriority.Highest;
        

        public DynamoStore(
            ILogger logger,
            IConfigurationManager configurationManager,
            IContainerFacade containerFacade,
            IDynamoClient dynamoClient
         ) 
        {
            _containerFacade = containerFacade;
            Logger = logger;
            _redisConn = ConnectionMultiplexer.Connect(configurationManager.AppSettings["RedisConnectionString"]);
            _context = dynamoClient.GetDatabase();
        }
        
        public void Start()
        {
            var dataEntities = _containerFacade.GetAllInstances<DataEntity>();
            foreach (var dataEntity in dataEntities)
            {
                var method = typeof(DynamoStore).GetMethod(nameof(GetOrCreateTable), BindingFlags.NonPublic | BindingFlags.Instance);
                if (method != null) method.MakeGenericMethod(dataEntity.GetType()).Invoke(this, new object[] { });
            }
            SetupHealthCheck();
        }

        private void SetupHealthCheck()
        {
            var record = new HealthCheckEntity(){Id = "1",CreatedAt = DateTime.Now, ExpiresAt = DateTime.Now.AddYears(10)};
            
            _context.CreateTableIfNotExists(new CreateTableArgs<HealthCheckEntity>(typeof(HealthCheckEntity).Name, typeof(string), g => g.Id ));
            var table = _context.GetTable<HealthCheckEntity>();
            var entity = table.FirstOrDefault(x => (string)x.Id == "1");
            if (entity == null)
            {
                table.InsertOnSubmit(record);
                _context.SubmitChanges();
            }
            Logger.Trace($"{nameof(DynamoStore)}.{nameof(SetupHealthCheck)}",
                new LogItem("Event", "Insert health check entity"),
                new LogItem("Type", typeof(HealthCheckEntity).ToString),
                new LogItem("Entity", record.ToString));
        }

        /// <summary>
        /// Puts the given DataEntity into its corresponding Dynamo table
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entityToStore"></param>
        public void Put<T>(T entityToStore) where T : DataEntity
        {
            var table = GetCachedTable<T>();
            table.InsertOnSubmit(entityToStore);
            Stopwatch timer = Stopwatch.StartNew();
    
            try
            {
                _context.SubmitChanges();
                Logger.Trace($"{nameof(DynamoStore)}.{nameof(Put)}",
                    new LogItem("Event", "Insert entity"),
                    new LogItem("Type", typeof(T).ToString),
                    new LogItem("Entity", entityToStore.ToString),
                    new LogItem("DurationMilliseconds", timer.Elapsed.TotalMilliseconds));
            }
            catch (Exception ex)
            {
                Logger.Error($"{nameof(DynamoStore)}.{nameof(Put)}",
                    new LogItem("Event", "Failed to insert entity"),
                    new LogItem("Type", typeof(T).ToString),
                    new LogItem("Entity", entityToStore.ToString),
                    new LogItem("Exception Message", ex.Message),
                    new LogItem("Stack Trace", ex.StackTrace),
                    new LogItem("DurationMilliseconds", timer.Elapsed.TotalMilliseconds));
                throw;
            }
        }

        /// <summary>
        /// Replaces all items matching the filter with the given DataEntity in the corresponding Dynamo table
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entityToStore"></param>
        /// <param name="filter"></param>
        public void Put<T>(T entityToStore, Expression<Func<T, bool>> filter) where T : DataEntity
        {
            var table = this.GetCachedTable<T>();
            var deleteResult = table.AsQueryable().Where(filter);
            
            foreach (var dataEntity in deleteResult)
            {
                table.RemoveOnSubmit(dataEntity);
            }
            table.InsertOnSubmit(entityToStore);

            Stopwatch timer = Stopwatch.StartNew();
            try
            {
                _context.SubmitChanges();
                Logger.Trace($"{nameof(DynamoStore)}.{nameof(Put)}",
                    new LogItem("Event", "Insert entity"),
                    new LogItem("Type", typeof(T).ToString),
                    new LogItem("Entity", entityToStore.ToString),
                    new LogItem("Filter", filter.Body.ToString),
                    new LogItem("DurationMilliseconds", timer.Elapsed.TotalMilliseconds));
            }
            catch (Exception ex)
            {
                Logger.Error($"{nameof(DynamoStore)}.{nameof(Put)}",
                    new LogItem("Event", "Failed to insert entity"),
                    new LogItem("Type", typeof(T).ToString),
                    new LogItem("Entity", entityToStore.ToString),
                    new LogItem("Filter", filter.Body.ToString),
                    new LogItem("Exception Message", ex.Message),
                    new LogItem("Stack Trace", ex.StackTrace),
                    new LogItem("DurationMilliseconds", timer.Elapsed.TotalMilliseconds));
                throw;
            }
        }

        /// <summary>
        /// Gets an IQueryable collection of the DataEntity requested. 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public IQueryable<T> QueryableCollection<T>() where T : DataEntity
        {
            Logger.Trace($"{nameof(DynamoStore)}.{nameof(QueryableCollection)}",
                new LogItem("Event", "Get table as queryable"), new LogItem("Type", typeof(T).ToString));
            return GetCachedTable<T>().AsQueryable();
        }
        
        /// <summary>
        /// Removes all items matching the filter in the corresponding Dynamo table
        /// </summary>
        /// <param name="filter"></param>
        /// <typeparam name="T"></typeparam>
        public void Remove<T>(Expression<Func<T, bool>> filter) where T : DataEntity
        {
            var table = this.GetCachedTable<T>();
            var deleteResult = table.AsQueryable().Where(filter);
            
            foreach (var dataEntity in deleteResult)
            {
                table.RemoveOnSubmit(dataEntity);
                Logger.Trace($"{nameof(DynamoStore)}.{nameof(Remove)}",
                    new LogItem("Event", "Remove entity"),
                    new LogItem("Type", typeof(T).ToString),
                    new LogItem("Entity", dataEntity.ToString),
                    new LogItem("Filter", filter.Body.ToString));
            }

            Stopwatch timer = Stopwatch.StartNew();
            try
            {
                _context.SubmitChanges();
                Logger.Trace($"{nameof(DynamoStore)}.{nameof(Remove)}",
                    new LogItem("Event", "Remove entity success"),
                    new LogItem("Type", typeof(T).ToString),
                    new LogItem("Filter", filter.Body.ToString),
                    new LogItem("DurationMilliseconds", timer.Elapsed.TotalMilliseconds));
            }
            catch (Exception ex)
            {
                Logger.Error($"{nameof(DynamoStore)}.{nameof(Put)}",
                    new LogItem("Event", "Failed to insert entity"),
                    new LogItem("Type", typeof(T).ToString),
                    new LogItem("Filter", filter.Body.ToString),
                    new LogItem("Exception Message", ex.Message),
                    new LogItem("Stack Trace", ex.StackTrace),
                    new LogItem("DurationMilliseconds", timer.Elapsed.TotalMilliseconds));
                throw;
            }
        }
        
        /// <summary>
        /// Updates the given DataEntity in the corresponding Dynamo table 
        /// </summary>
        /// <param name="entityToStore"></param>
        /// <typeparam name="T"></typeparam>
        public void Update<T>(T entityToStore) where T : DataEntity
        {
            var table = this.GetCachedTable<T>();
            var oldEntity = table.FirstOrDefault(x => x.Id == entityToStore.Id);
            table.RemoveOnSubmit(oldEntity);
            table.InsertOnSubmit(entityToStore);

            Stopwatch timer = Stopwatch.StartNew();
            try
            {
                _context.SubmitChanges();

                Logger.Trace($"{nameof(DynamoStore)}.{nameof(Update)}",
                    new LogItem("Event", "Update entity"),
                    new LogItem("Type", typeof(T).ToString),
                    new LogItem("Entity", entityToStore.ToString),
                    new LogItem("DurationMilliseconds", timer.Elapsed.TotalMilliseconds));
            }
            catch (Exception ex)
            {
                Logger.Error($"{nameof(DynamoStore)}.{nameof(Update)}",
                    new LogItem("Event", "Failed to update entity"),
                    new LogItem("Type", typeof(T).ToString),
                    new LogItem("Entity", entityToStore.ToString),
                    new LogItem("Exception Message", ex.Message),
                    new LogItem("Stack Trace", ex.StackTrace),
                    new LogItem("DurationMilliseconds", timer.Elapsed.TotalMilliseconds));
                throw;
            }
        }
        
        /// <summary>
        /// Removes DataEntity with the given Id from the corresponding Dynamo table 
        /// </summary>
        /// <param name="id"></param>
        /// <typeparam name="T"></typeparam>
        public void Remove<T>(object id) where T : DataEntity
        {
            var table = this.GetCachedTable<T>();
            var entity = table.Find(id);
            table.RemoveOnSubmit(entity);

            Stopwatch timer = Stopwatch.StartNew();
            try
            {
                _context.SubmitChanges();

                Logger.Trace($"{nameof(DynamoStore)}.{nameof(Remove)}",
                    new LogItem("Event", "Remove entity"),
                    new LogItem("Type", typeof(T).ToString),
                    new LogItem("Id", id.ToString),
                    new LogItem("DurationMilliseconds", timer.Elapsed.TotalMilliseconds));
            }
            catch (Exception ex)
            {
                Logger.Error($"{nameof(DynamoStore)}.{nameof(Remove)}",
                    new LogItem("Event", "Failed to remove entity"),
                    new LogItem("Type", typeof(T).ToString),
                    new LogItem("Entity", entity.ToString),
                    new LogItem("Exception Message", ex.Message),
                    new LogItem("Stack Trace", ex.StackTrace),
                    new LogItem("DurationMilliseconds", timer.Elapsed.TotalMilliseconds));
                throw;
            }
        }
        /// <summary>
        /// Removes the given DataEntity in the corresponding Dynamo table
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entityToRemove"></param>
        public void RemoveEntity<T>(T entityToRemove) where T : DataEntity
        {
            Remove<T>(entityToRemove.Id);
        }

        /// <summary>
        /// Returns the number of items of the requested type in the Dynamo table.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public long Count<T>() where T : DataEntity
        {
            Logger.Trace($"{nameof(DynamoStore)}.{nameof(Count)}",
                new LogItem("Event", "Get collection count"), new LogItem("Type", typeof(T).ToString));
            return GetCachedTable<T>().Count(arg => true);
        }

        /// <summary>
        /// Returns the number of items of the requested type in the Dynamo table filtered by the given expression.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public long Count<T>(Expression<Func<T, bool>> expression) where T : DataEntity
        {
            Logger.Trace($"{nameof(DynamoStore)}.{nameof(Count)}",
                new LogItem("Event", "Get collection count"), new LogItem("Type", typeof(T).ToString),
                new LogItem("Expression", expression.ToString));
            return GetCachedTable<T>().Count(expression);
        }
        
        /// <summary>
        /// Gets the requested item of requested type from Dynamo table
        /// </summary>
        /// <param name="id"></param>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public T Get<T>(object id) where T : DataEntity
        {
            var table = this.GetCachedTable<T>();
            Stopwatch timer = Stopwatch.StartNew();
            var entity = table.FirstOrDefault(x => x.Id == id);

            Logger.Trace($"{nameof(DynamoStore)}.{nameof(Get)}",
                new LogItem("Event", "Get entity"),
                new LogItem("Type", typeof(T).ToString),
                new LogItem("Id", id.ToString),
                new LogItem("DurationMilliseconds", timer.Elapsed.TotalMilliseconds));
            
            return entity;
        }

        /// <summary>
        /// Gets an IEnumerable collection items matching the filter in the corresponding Dynamo table
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="filter"></param>
        public IEnumerable<T> Get<T>(Expression<Func<T, bool>> filter) where T : DataEntity
        {
            Logger.Trace($"{nameof(DynamoStore)}.{nameof(Get)}",
                new LogItem("Event", "Get entity"),
                new LogItem("Type", typeof(T).ToString),
                new LogItem("Filter", filter.Body.ToString));
            return this.GetCachedTable<T>().Where(filter);
        }
        /// <summary>
        /// Gets an IEnumerable collection of tables of a given type in the corresponding Dynamo database
        /// </summary>
        /// <typeparam name="T"></typeparam>
        public IEnumerable<T> GetTable<T>() where T : DataEntity
        {
            Logger.Trace($"{nameof(DynamoStore)}.{nameof(GetTable)}",
                new LogItem("Event", "Get table"),
                new LogItem("Type", typeof(T).ToString));
            return this.GetCachedTable<T>();
        }

        /// <summary>
        /// Gets all items matching the filter of the given type in the corresponding Dynamo table
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="filter"></param>
        public IList<T> GetList<T>(Expression<Func<T, bool>> filter)
            where T : DataEntity
        {
            Logger.Trace($"{nameof(DynamoStore)}.{nameof(GetList)}",
                new LogItem("Event", "Get list"),
                new LogItem("Type", typeof(T).ToString),
                new LogItem("Filter", filter.Body.ToString));
            return Get(filter).ToList();
        }
        /// <summary>
        /// Checks connection to Dynamo is active
        /// </summary>
        /// <returns></returns>
        public bool Ping()
        {
            var table = _context.GetTable<HealthCheckEntity>();
            var record = table.FirstOrDefault(x => (string)x.Id == "1");
            return record != null;
        }

        private void GetOrCreateTable<T>() where T : DataEntity
        {
            _context.CreateTableIfNotExists(new CreateTableArgs<T>(typeof(T).Name, typeof(string), g => g.Id ));
            Stopwatch timer = Stopwatch.StartNew();

            try
            {
                _context.SubmitChanges();
                Logger.Trace($"{nameof(DynamoStore)}.{nameof(GetCachedTable)}<{typeof(T).Name}>",
                    new LogItem("Action", "Getting or Creating table"),
                    new LogItem("Type", typeof(T).ToString),
                    new LogItem("DurationMilliseconds", timer.Elapsed.TotalMilliseconds));
            }
            catch (Exception ex)
            {
                Logger.Error($"{nameof(DynamoStore)}.{nameof(GetCachedTable)}<{typeof(T).Name}>",
                    new LogItem("Action", "Failed to create table"),
                    new LogItem("Type", typeof(T).ToString),
                    new LogItem("Exception Message", ex.Message),
                    new LogItem("Stack Trace", ex.StackTrace),
                    new LogItem("DurationMilliseconds", timer.Elapsed.TotalMilliseconds));
                throw;
            }
            
            _context.GetTable<T>(typeof(T).Name, () => new RedisTableCache(_redisConn));
           var table = _context.GetTable<T>();
           TableCache.Add(typeof(T),table);
        }

        private DataTable<T> GetCachedTable<T>() where T : DataEntity
        {
            lock (TableCache)
            {
                if (!TableCache.ContainsKey(typeof(T)))
                {
                    Logger.Trace($"{nameof(DynamoStore)}.{nameof(GetCachedTable)}<{typeof(T).Name}>",
                        new LogItem("Action", "Not cached, call GetOrCreateTable"));
                    GetOrCreateTable<T>();
                }

                return (DataTable<T>)TableCache[typeof(T)];
            }
        }
        
        private static string FormatEntity<T>(T entity, Helpers.OperationType type)
        {
            var regex = new Regex("ISODate[(](.+?)[)]");

            var result = new
            {
                Service = ServiceInfo.Name,
                OperationType = Enum.GetName(typeof(OperationType), type),
                Entity = JsonConvert.SerializeObject(entity),
                EntityType = typeof(T).Name,
                Date = DateTime.UtcNow
            };

            return regex.Replace(JsonConvert.SerializeObject(result),"$1");
        }
    }
}