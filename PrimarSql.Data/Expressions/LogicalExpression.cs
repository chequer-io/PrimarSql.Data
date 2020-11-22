namespace PrimarSql.Data.Expressions
{
    internal sealed class LogicalExpression : IExpression
    {
        public IExpression Left { get; set; }
        
        public string Operator { get; set; }
        
        public IExpression Right { get; set; }
    }
}
