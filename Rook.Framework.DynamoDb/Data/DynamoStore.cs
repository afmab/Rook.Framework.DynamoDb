using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;
using Amazon.DynamoDBv2;
using Linq2DynamoDb.DataContext;
using Linq2DynamoDb.DataContext.Caching.Redis;
using Newtonsoft.Json;
using Rook.Framework.Core.AmazonKinesisFirehose;
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
        private static  AmazonDynamoDBClient _client;
        private readonly IContainerFacade _containerFacade;
        internal readonly ILogger Logger;
        private readonly IAmazonFirehoseProducer _amazonFirehoseProducer; 
        private readonly string _amazonKinesisStreamName;
        private static  ConnectionMultiplexer _redisConn;
        internal static Dictionary<Type, object> TableCache { get; } = new Dictionary<Type, object>();
        public StartupPriority StartupPriority { get; } = StartupPriority.Highest;
        

        public DynamoStore(
            ILogger logger,
            IConfigurationManager configurationManager,
            IContainerFacade containerFacade,
            IAmazonFirehoseProducer amazonFirehoseProducer
         ) 
        {
            _client = new AmazonDynamoDBClient();
            _containerFacade = containerFacade;
            Logger = logger;
            _amazonFirehoseProducer = amazonFirehoseProducer;
            _amazonKinesisStreamName = configurationManager.Get<string>("RepositoryKinesisStream");
            _redisConn = ConnectionMultiplexer.Connect(configurationManager.AppSettings["RedisConnectionString"]);
            _client = new AmazonDynamoDBClient(
                configurationManager.Get<string>("AWSAccessKey"),
                configurationManager.Get<string>("AWSSecretKey"));
            _context = new DataContext(_client,String.Empty);

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
                
                try
                {
                    _amazonFirehoseProducer.PutRecord(_amazonKinesisStreamName,
                        FormatEntity(entityToStore, Helpers.OperationType.Insert));
                }
                catch (Exception ex)
                {
                    Logger.Error($"{nameof(DynamoStore)}.{nameof(Put)}",
                        new LogItem("Event", "Failed to send update to data lake"),
                        new LogItem("Type", typeof(T).ToString),
                        new LogItem("Entity", entityToStore.ToString),
                        new LogItem("Exception Message", ex.Message),
                        new LogItem("Stack Trace", ex.StackTrace),);
                }
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

                try
                {
                    _amazonFirehoseProducer.PutRecord(_amazonKinesisStreamName,
                        deleteResult.Count() != 0
                            ? FormatEntity(entityToStore, Helpers.OperationType.Update)
                            : FormatEntity(entityToStore, Helpers.OperationType.Insert));
                }
                catch (Exception ex)
                {
                    Logger.Error($"{nameof(DynamoStore)}.{nameof(Put)}",
                        new LogItem("Event", "Failed to send update to data lake"),
                        new LogItem("Type", typeof(T).ToString),
                        new LogItem("Entity", entityToStore.ToString),
                        new LogItem("Exception Message", ex.Message),
                        new LogItem("Stack Trace", ex.StackTrace));
                }
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

        public IQueryable<T> QueryableCollection<T>() where T : DataEntity
        {
            Logger.Trace($"{nameof(DynamoStore)}.{nameof(QueryableCollection)}",
                new LogItem("Event", "Get table as queryable"), new LogItem("Type", typeof(T).ToString));
            return GetCachedTable<T>().AsQueryable();
            
        }

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

                try
                {
                    _amazonFirehoseProducer.PutRecord(_amazonKinesisStreamName, FormatEntity(entityToStore, Helpers.OperationType.Update));
                }
                catch (Exception ex)
                {
                    Logger.Error($"{nameof(DynamoStore)}.{nameof(Update)}",
                        new LogItem("Event", "Failed to send update to data lake"),
                        new LogItem("Type", typeof(T).ToString),
                        new LogItem("Entity", entityToStore.ToString),
                        new LogItem("Exception Message", ex.Message),
                        new LogItem("Stack Trace", ex.StackTrace),
                        new LogItem("DurationMilliseconds", timer.Elapsed.TotalMilliseconds));
                }
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

                try
                {
                    _amazonFirehoseProducer.PutRecord(_amazonKinesisStreamName, FormatEntity(entity, Helpers.OperationType.Remove));
                }
                catch (Exception ex)
                {
                    Logger.Error($"{nameof(DynamoStore)}.{nameof(Remove)}",
                        new LogItem("Event", "Failed to send update to data lake"),
                        new LogItem("Type", typeof(T).ToString),
                        new LogItem("Entity", entity.ToString),
                        new LogItem("Exception Message", ex.Message),
                        new LogItem("Stack Trace", ex.StackTrace),
                        new LogItem("DurationMilliseconds", timer.Elapsed.TotalMilliseconds));
                }
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
        
        public void RemoveEntity<T>(T entityToRemove) where T : DataEntity
        {
            Remove<T>(entityToRemove.Id);
        }

        public long Count<T>() where T : DataEntity
        {
            Logger.Trace($"{nameof(DynamoStore)}.{nameof(Count)}",
                new LogItem("Event", "Get collection count"), new LogItem("Type", typeof(T).ToString));
            return GetCachedTable<T>().Count(arg => true);
        }

        public long Count<T>(Expression<Func<T, bool>> expression) where T : DataEntity
        {
            Logger.Trace($"{nameof(DynamoStore)}.{nameof(Count)}",
                new LogItem("Event", "Get collection count"), new LogItem("Type", typeof(T).ToString),
                new LogItem("Expression", expression.ToString));
            return GetCachedTable<T>().Count(expression);
        }
        
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

        public IEnumerable<T> Get<T>(Expression<Func<T, bool>> filter) where T : DataEntity
        {
            Logger.Trace($"{nameof(DynamoStore)}.{nameof(Get)}",
                new LogItem("Event", "Get entity"),
                new LogItem("Type", typeof(T).ToString),
                new LogItem("Filter", filter.Body.ToString));
            return this.GetCachedTable<T>().Where(filter);
        }

        public IEnumerable<T> GetTable<T>() where T : DataEntity
        {
            Logger.Trace($"{nameof(DynamoStore)}.{nameof(GetTable)}",
                new LogItem("Event", "Get table"),
                new LogItem("Type", typeof(T).ToString));
            return this.GetCachedTable<T>();
        }

        public IList<T> GetList<T>(Expression<Func<T, bool>> filter)
            where T : DataEntity
        {
            Logger.Trace($"{nameof(DynamoStore)}.{nameof(GetList)}",
                new LogItem("Event", "Get list"),
                new LogItem("Type", typeof(T).ToString),
                new LogItem("Filter", filter.Body.ToString));
            return Get(filter).ToList();
        }

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