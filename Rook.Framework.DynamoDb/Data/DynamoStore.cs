using System;
using System.Collections.Generic;
using System.Reflection;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Newtonsoft.Json;
using Rook.Framework.Core.Common;
using Rook.Framework.Core.Services;
using Rook.Framework.Core.StructureMap;
using Rook.Framework.DynamoDb.Utilities;

namespace Rook.Framework.DynamoDb.Data
{
    public sealed class DynamoStore : IStartable, IDynamoStore
    {
        private AmazonDynamoDBClient DynamoConnection { get; set; }
        private readonly IDynamoClient _client;
        private readonly IContainerFacade _containerFacade;
        internal readonly ILogger Logger;
        internal static Dictionary<Type, object> TableCache { get; } = new Dictionary<Type, object>();
        public StartupPriority StartupPriority { get; } = StartupPriority.Highest;
        

        public DynamoStore(
            ILogger logger,
            IDynamoClient client,
            IContainerFacade containerFacade
         )
        {
            _client = client;
            _containerFacade = containerFacade;
            Logger = logger;
            _client.Create();
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
            Table table = GetCachedTable<T>();
            
            DynamoDBContext db = new DynamoDBContext(DynamoConnection);
            Document document = db.ToDocument(entityToStore);
            table.PutItemAsync(document);
            Logger.Trace($"{nameof(DynamoStore)}.{nameof(Put)}",
                new LogItem("Event", "Insert entity"),
                new LogItem("Type", typeof(T).ToString),
                new LogItem("Entity", entityToStore.ToString));
        }

        public bool Ping()
        {
            //TODO: implement a proper check here 
            Connect();
            return true;
        }

        public void Remove<T>(object id) where T : DataEntity
        {
            Table table = GetCachedTable<T>();
            table.DeleteItemAsync((Guid)id);
            Logger.Trace($"{nameof(DynamoStore)}.{nameof(Remove)}",
                new LogItem("Event", "Remove entity"),
                new LogItem("Type", typeof(T).ToString),
                new LogItem("Id", id.ToString));
        }

        public T Get<T>(object id) where T : DataEntity
        {
            Table table = GetCachedTable<T>();
            var document = table.GetItemAsync((Guid)id);
            Logger.Trace($"{nameof(DynamoStore)}.{nameof(Get)}",
                new LogItem("Event", "Get entity"),
                new LogItem("Type", typeof(T).ToString),
                new LogItem("Id", id.ToString));
            return JsonConvert.DeserializeObject<T>(document.Result);
        }
        
        private void Connect()
        {
            if (DynamoConnection == null)
                DynamoConnection = _client.GetDatabase();
                    
        }


        private void GetOrCreateTable<T>()
        {
            Connect();
            
           if (!GetCachedTables().Contains(typeof(T).Name)) 
               CreateDynamoTable<T>();
           
           var  table = Table.LoadTable(DynamoConnection, typeof(T).Name);
           TableCache.Add(typeof(T),table);
        }

        private void CreateDynamoTable<T>()
        {
            var attributeDefinitions = new List<AttributeDefinition>();
            PropertyInfo[] members = typeof(T).GetProperties();
            foreach (PropertyInfo memberInfo in members)
            {
                var attribute = new AttributeDefinition()
                {
                    AttributeName = memberInfo.Name,
                    AttributeType = DynamoTypeMapper.GetDynamoType(memberInfo.PropertyType)
                };
                attributeDefinitions.Add(attribute);
            }

            var request = new CreateTableRequest
            {
                TableName = typeof(T).Name,
                AttributeDefinitions = attributeDefinitions,
                KeySchema = new List<KeySchemaElement>()
                {
                    new KeySchemaElement
                    {
                        AttributeName = "Id",
                        KeyType = "HASH" //Partition key
                    }
                },
                ProvisionedThroughput = new ProvisionedThroughput
                {
                    ReadCapacityUnits = 10,
                    WriteCapacityUnits = 5
                }
            };
            var response = DynamoConnection.CreateTableAsync(request).Result;
        }

        private List<string> GetCachedTables()
        {
            var tableNames = new List<string>();
            string lastEvaluatedTableName = null;
            do
            {
                var request = new ListTablesRequest
                {
                    Limit = 10, 
                    ExclusiveStartTableName = lastEvaluatedTableName
                };

                var response = DynamoConnection.ListTablesAsync(request).Result;
                var results = response.TableNames;
                tableNames.AddRange(results);
                lastEvaluatedTableName = response.LastEvaluatedTableName;

            } while (lastEvaluatedTableName != null);

            return tableNames;
        }
        
        private Table GetCachedTable<T>() where T : DataEntity
        {
            lock (TableCache)
            {
                if (!TableCache.ContainsKey(typeof(T)))
                {
                    Logger.Trace($"{nameof(DynamoStore)}.{nameof(GetCachedTable)}<{typeof(T).Name}>",
                        new LogItem("Action", "Not cached, call GetOrCreateCollection"));
                    GetOrCreateTable<T>();
                }

                return (Table)TableCache[typeof(T)];
            }
        }
    }
}