using PrimarSql.Data.Models.Columns;

namespace PrimarSql.Data.Expressions
{
    internal sealed class MemberExpression : IExpression
    {
        public IPart[] Name { get; set; }
    }
}
