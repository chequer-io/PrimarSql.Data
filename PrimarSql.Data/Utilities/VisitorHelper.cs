using System;
using PrimarSql.Data.Expressions;
using PrimarSql.Data.Models.Columns;

namespace PrimarSql.Data.Utilities
{
    internal static class VisitorHelper
    {
        public static readonly string[] StringTypes =
        {
            "varchar", "text", "mediumtext", "longtext", "string"
        };

        public static readonly string[] NumberTypes =
        {
            "int", "integer", "bigint"
        };

        public static MemberExpression GetMemberByName(string name)
        {
            return new MemberExpression
            {
                Name = new IPart[] { new IdentifierPart(name) }
            };
        }

        public static string DataTypeToDynamoDBType(string dataType)
        {
            dataType = dataType.ToLower();

            switch (dataType)
            {
                case "binary":
                    return "B";

                case "bool":
                case "boolean":
                    return "BOOL";

                case "binary_list":
                    return "BS";

                case "list":
                    return "L";

                case "object":
                    return "M";

                case "int":
                case "integer":
                case "bigint":
                    return "N";

                case "number_list":
                    return "NS";
                
                case "null":
                    return "NULL";
                
                case "varchar":
                case "text":
                case "mediumtext":
                case "longtext":
                case "string":
                    return "S";
                
                case "string_list":
                    return "SS";
            }
            
            throw new InvalidOperationException($"Unknown {dataType} type.");
        }
    }
}
