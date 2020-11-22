namespace PrimarSql.Data.Expressions
{
    internal sealed class InExpression : IExpression
    {
        public IExpression Target { get; set; }
        
        public IExpression Sources { get; set; }
    }
}
