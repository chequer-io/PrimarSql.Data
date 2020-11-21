using PrimarSql.Data.Models;

namespace PrimarSql.Data.Expressions
{
    public class SelectExpression : IExpression
    {
        public SelectQueryInfo SelectQueryInfo { get; set; }
    }
}
