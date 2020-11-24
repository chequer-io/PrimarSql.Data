using PrimarSql.Data.Expressions;

namespace PrimarSql.Data.Planners
{
    internal sealed class DeleteQueryInfo : IQueryInfo
    {
        public string TableName { get; set; }

        public IExpression WhereExpression { get; set; }

        public int Limit { get; set; } = -1;
    }
}
