using System.Collections.Generic;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Expressions;
using PrimarSql.Data.Models.Columns;

namespace PrimarSql.Data.Planners
{
    internal sealed class UpdateQueryInfo : IQueryInfo
    {
        public string TableName { get; set; }

        public IExpression WhereExpression { get; set; }

        public IEnumerable<UpdateElement> UpdatedElements { get; set; }

        public int Limit { get; set; } = -1;
    }
}
