using PrimarSql.Data.Utilities;

namespace PrimarSql.Data.Models.Conditions
{
    internal class BetweenCondition : ICondition
    {
        public bool IsActivated => true;

        public ICondition Predicate { get; set; }

        public ICondition Expression1 { get; set; }

        public ICondition Expression2 { get; set; }

        public string ToExpression(AttributeValueManager valueManager)
        {
            var predicate = Predicate.ToExpression(valueManager);
            var lowValue = Expression1.ToExpression(valueManager);
            var highValue = Expression2.ToExpression(valueManager);

            return $"{predicate} BETWEEN {lowValue} AND {highValue}";
        }
    }
}
