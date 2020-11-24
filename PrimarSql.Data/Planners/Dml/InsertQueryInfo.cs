using System.Collections.Generic;
using PrimarSql.Data.Expressions;

namespace PrimarSql.Data.Planners
{
    internal sealed class InsertQueryInfo : IQueryInfo
    {
        public string TableName { get; set; }
        
        public bool IgnoreDuplicate { get; set; }
        
        public string[] Columns { get; set; }

        public bool FromSelectQuery => SelectQueryInfo != null;
        
        public SelectQueryInfo SelectQueryInfo { get; set; }
        
        public IEnumerable<IEnumerable<IExpression>> Rows { get; set; }
    }
}
