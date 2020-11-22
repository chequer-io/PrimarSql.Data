using PrimarSql.Data.Models;

namespace PrimarSql.Data.Expressions
{
    internal sealed class SelectExpression : IExpression
    {
        public SelectQueryInfo SelectQueryInfo { get; set; }
    }
}
