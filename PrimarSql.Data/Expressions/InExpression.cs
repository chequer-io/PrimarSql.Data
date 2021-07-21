namespace PrimarSql.Data.Expressions
{
    internal sealed class InExpression : IExpression
    {
        public bool IsNot { get; set; }

        public IExpression Target { get; set; }

        public IExpression Sources { get; set; }
    }
}
