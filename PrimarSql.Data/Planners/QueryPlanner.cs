using System;
using System.Data.Common;
using Amazon.DynamoDBv2;
using System.Linq;
using PrimarSql.Data.Models;

namespace PrimarSql.Data.Planners
{
    internal abstract class QueryPlanner<T> : IQueryPlanner where T : IQueryInfo
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
        
        IQueryInfo IQueryPlanner.QueryInfo => QueryInfo;

        public T QueryInfo { get; set; }
        
        public QueryContext Context { get; set; }
        
        public abstract DbDataReader Execute();
    }
}
