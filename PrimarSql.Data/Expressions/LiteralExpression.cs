using PrimarSql.Data.Models;

namespace PrimarSql.Data.Expressions
{
    internal sealed class LiteralExpression : IExpression
    {
        public object Value { get; set; }
        
        public LiteralValueType ValueType { get; set; }
    }
}
