using System;

namespace Rook.Framework.DynamoDb.Utilities
{
    public static class DynamoTypeMapper
    {
        public static string GetDynamoType(object typeTocheck)
        {
            //done as if else because it is much faster than a switch on type checking

            if (typeTocheck is string || typeTocheck is char || typeTocheck is DateTime)
            {
                return "S";
            }
            else if (typeTocheck is int || typeTocheck is double || typeTocheck is decimal || typeTocheck is float || typeTocheck is short)
            {
                return "N";
            }
            else
            {
                return "B";
            }

            
        }
    }
}