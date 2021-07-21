using PrimarSql.Data.Utilities;

namespace PrimarSql.Data.Models.Conditions
{
    internal class UnaryCondition : ICondition
    {
        public bool IsActivated => true;

        public string Operator { get; set; }

        public ICondition Condition { get; set; }

        public string ToExpression(AttributeValueManager valueManager)
        {
            return $"{Operator}({Condition.ToExpression(valueManager)})";
        }
    }
}
