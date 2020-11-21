using PrimarSql.Data.Utilities;

namespace PrimarSql.Data.Models.Conditions
{
    public class NestedCondition : ICondition
    {
        public ICondition Condition { get; }

        public bool IsActivated => Condition.IsActivated;

        public NestedCondition(ICondition condition)
        {
            Condition = condition;
        }

        public string ToExpression(AttributeValueManager valueManager)
        {
            return IsActivated ? $"({Condition.ToExpression(valueManager)})" : null;
        }
    }
}
