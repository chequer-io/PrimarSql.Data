namespace PrimarSql.Data.Expressions
{
    public class FunctionExpression : IExpression
    {
        public IExpression Member { get; set; }
        
        public IExpression[] Parameters { get; set; }
    }
}
