namespace PrimarSql.Data.Expressions
{
    internal sealed class BetweenExpression : IExpression
    {
        public IExpression Target { get; set; }
        
        public bool IsNot { get; set; }
        
        public IExpression Expression1 { get; set; }
        
        public IExpression Expression2 { get; set; }
    }
}
