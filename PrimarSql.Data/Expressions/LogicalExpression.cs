namespace PrimarSql.Data.Expressions
{
    public class LogicalExpression : IExpression
    {
        public IExpression Left { get; set; }
        
        public string Operator { get; set; }
        
        public IExpression Right { get; set; }
    }
}
