using System;
using System.Collections.Generic;
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
    //TODO: Error checking on external calls
    //TODO: Metrics on external calls
    //TODO: More Logging
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
        }
        
        public void Put<T>(T entityToStore) where T : DataEntity
        {
            var table = GetCachedTable<T>();
            table.InsertOnSubmit(entityToStore);
            _context.SubmitChanges();

            _amazonFirehoseProducer.PutRecord(_amazonKinesisStreamName,
                FormatEntity(entityToStore, Helpers.OperationType.Insert));
            
            Logger.Trace($"{nameof(DynamoStore)}.{nameof(Put)}",
                new LogItem("Event", "Insert entity"),
                new LogItem("Type", typeof(T).ToString),
                new LogItem("Entity", entityToStore.ToString));
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
            _context.SubmitChanges();
            
            _amazonFirehoseProducer.PutRecord(_amazonKinesisStreamName,
                deleteResult.Count() != 0
                    ? FormatEntity(entityToStore, Helpers.OperationType.Update)
                    : FormatEntity(entityToStore, Helpers.OperationType.Insert));
            
            Logger.Trace($"{nameof(DynamoStore)}.{nameof(Put)}",
                new LogItem("Event", "Insert entity"),
                new LogItem("Type", typeof(T).ToString),
                new LogItem("Entity", entityToStore.ToString),
                new LogItem("Filter", filter.Body.ToString));
        }

        public IQueryable<T> QueryableCollection<T>() where T : DataEntity
        {
            throw new NotImplementedException();
        }

        public void Remove<T>(Expression<Func<T, bool>> filter) where T : DataEntity
        {
            throw new NotImplementedException();
        }
//TODO: this one too
//        public void Update<T>(Expression<Func<T, bool>> filter, UpdateDefinition<T> updates) where T : DataEntity
//        {
//            
//        }

        public void Remove<T>(object id) where T : DataEntity
        {
            var table = this.GetCachedTable<T>();
            var entity = table.Find(id);
            table.RemoveOnSubmit(entity);
            _context.SubmitChanges();

            Logger.Trace($"{nameof(DynamoStore)}.{nameof(Remove)}",
                new LogItem("Event", "Remove entity"),
                new LogItem("Type", typeof(T).ToString),
                new LogItem("Id", id.ToString));
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
            var entity = table.FirstOrDefault(x => x.Id == id);

            Logger.Trace($"{nameof(DynamoStore)}.{nameof(Get)}",
                new LogItem("Event", "Get entity"),
                new LogItem("Type", typeof(T).ToString),
                new LogItem("Id", id.ToString));
            
            return entity;
        }

        public IEnumerable<T> Get<T>(Expression<Func<T, bool>> filter) where T : DataEntity
        {
            return this.GetCachedTable<T>().Where(filter);
        }

        public IEnumerable<T> GetTable<T>() where T : DataEntity
        {
            return this.GetCachedTable<T>();
        }

        public IList<T> GetList<T>(Expression<Func<T, bool>> filter)
            where T : DataEntity
        {
            return Get(filter).ToList();
        }

        public bool Ping()
        {
            //TODO: do this mark
            return true;
        }

        private void GetOrCreateTable<T>() where T : DataEntity
        {
            
            _context.CreateTableIfNotExists(new CreateTableArgs<T>(typeof(T).Name, typeof(string), g => g.Id ));
            _context.SubmitChanges();
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

            return regex.Replace(Newtonsoft.Json.JsonConvert.SerializeObject(result),"$1");
        }
    }
}