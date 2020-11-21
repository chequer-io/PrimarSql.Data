namespace PrimarSql.Data.Expressions
{
    public class InExpression : IExpression
    {
        public IExpression Target { get; set; }
        
        public IExpression Sources { get; set; }
    }
}
