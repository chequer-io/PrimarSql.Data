using System.Collections.Generic;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Expressions;

namespace PrimarSql.Data.Planners
{
    internal sealed class InsertQueryInfo : IQueryInfo
    {
        public InsertValueType InsertValueType
        {
            get
            {
                if (SelectQueryInfo != null)
                    return InsertValueType.Subquery;

                if (JsonValues != null)
                    return InsertValueType.JsonValues;

                return InsertValueType.RawValues;
            }
        }

        public string TableName { get; set; }

        public bool IgnoreDuplicate { get; set; }

        public string[] Columns { get; set; }

        public SelectQueryInfo SelectQueryInfo { get; set; }

        public IEnumerable<IEnumerable<IExpression>> Rows { get; set; }

        public IEnumerable<Dictionary<string, AttributeValue>> JsonValues { get; set; }
    }
}
