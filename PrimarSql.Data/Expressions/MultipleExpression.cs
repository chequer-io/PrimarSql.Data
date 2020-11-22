namespace PrimarSql.Data.Expressions
{
    internal sealed class MultipleExpression : IExpression
    {
        public IExpression[] Expressions { get; set; }
    }
}
