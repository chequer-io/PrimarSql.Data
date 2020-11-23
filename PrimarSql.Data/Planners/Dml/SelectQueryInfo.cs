using PrimarSql.Data.Expressions;
using PrimarSql.Data.Models.Columns;
using PrimarSql.Data.Sources;

namespace PrimarSql.Data.Planners
{
    internal sealed class SelectQueryInfo : IQueryInfo
    {
        public ITableSource TableSource { get; set; }
        
        public IColumn[] Columns { get; set; }
        
        public bool UseStronglyConsistent { get; set; } = false;

        public int Limit { get; set; } = -1;

        public int Offset { get; set; } = -1;

        public bool OrderDescend { get; set; } = false;
        
        public LiteralExpression StartHashKey { get; set; }
        
        public LiteralExpression StartSortKey { get; set; }

        public bool HasStartKey => StartHashKey != null;
        
        public IExpression WhereExpression { get; set; }
    }
}
