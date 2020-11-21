namespace PrimarSql.Data.Expressions
{
    public class MultipleExpression : IExpression
    {
        public IExpression[] Expressions { get; set; }
    }
}
