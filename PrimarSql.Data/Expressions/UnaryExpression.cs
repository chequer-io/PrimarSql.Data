namespace PrimarSql.Data.Expressions
{
    public class UnaryExpression : IExpression
    {
        public string Operator { get; set; }
        
        public IExpression Expression { get; set; }
    }
}
