namespace PrimarSql.Data.Expressions
{
    internal sealed class ArrayAppendExpression : IExpression
    {
        public MultipleExpression AppendItem { get; set; }
    }
}
