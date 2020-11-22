namespace PrimarSql.Data.Expressions
{
    internal sealed class FunctionExpression : IExpression
    {
        public IExpression Member { get; set; }
        
        public IExpression[] Parameters { get; set; }
    }
}
