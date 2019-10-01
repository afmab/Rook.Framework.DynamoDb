using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using Rook.Framework.Core.Services;
using Rook.Framework.DynamoDb.Data;
using StructureMap;

namespace Rook.Framework.DynamoDb.StructureMap
{
    public class DynamoRegistry : Registry
    {
        public DynamoRegistry() : this(Assembly.GetEntryAssembly())
        {
        }

        public DynamoRegistry(Assembly assmebly)
        {

            Scan(scan =>
            {
                scan.Assembly(assmebly);
                scan.AddAllTypesOf<DataEntity>();
            });

            For<IStartable>().Add<DynamoStore>();
            For<IDynamoStore>().ClearAll().Singleton().Use<DynamoStore>();
            For<IDynamoClient>().ClearAll().Singleton().Use<DynamoClient>();
        }
    }
}