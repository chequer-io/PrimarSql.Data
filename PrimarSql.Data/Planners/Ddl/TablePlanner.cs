using System;
using System.Linq;
using Amazon.DynamoDBv2;

namespace PrimarSql.Data.Planners
{
    internal abstract class TablePlanner<T> : QueryPlanner<T> where T : IQueryInfo
    {
        private readonly string[] _stringTypes =
        {
            "varchar", "text", "mediumtext", "longtext", "string"
        };

        private readonly string[] _numberTypes =
        {
            "int", "integer", "bigint"
        };

        protected ScalarAttributeType DataTypeToScalarAttributeType(string dataType)
        {
            dataType = dataType.ToLower();

            if (_stringTypes.Contains(dataType))
                return ScalarAttributeType.S;

            if (_numberTypes.Contains(dataType))
                return ScalarAttributeType.N;

            throw new NotSupportedException($"{dataType.ToUpper()} cannot use to Index Type.");
        }

    }
}
