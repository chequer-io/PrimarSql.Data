namespace PrimarSql.Data.Expressions
{
    internal sealed class UnaryExpression : IExpression
    {
        public string Operator { get; set; }
        
        public IExpression Expression { get; set; }
    }
}
