using PrimarSql.Data.Models;

namespace PrimarSql.Data.Expressions
{
    public class LiteralExpression : IExpression
    {
        public object Value { get; set; }
        
        public LiteralValueType ValueType { get; set; }
    }
}
