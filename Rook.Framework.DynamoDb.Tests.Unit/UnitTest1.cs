using NUnit.Framework;
using Rook.Framework.DynamoDb.Data;
using Rook.Framework.Core.Common;

namespace Tests
{
    public class Tests
    {
        [SetUp]
        public void Setup()
        {
        }

        [Test]
        public void Test1()
        {
            var test = new DynamoClient(new CofigurationManager());
            Assert.Pass();
        }
    }
}