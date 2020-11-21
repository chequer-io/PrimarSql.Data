namespace PrimarSql.Data.Expressions
{
    public class BetweenExpression : IExpression
    {
        public IExpression Target { get; set; }
        
        public bool IsNot { get; set; }
        
        public IExpression Expression1 { get; set; }
        
        public IExpression Expression2 { get; set; }
    }
}
