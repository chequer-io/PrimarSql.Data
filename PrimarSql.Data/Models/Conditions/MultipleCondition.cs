using System.Collections.Generic;
using System.Linq;
using PrimarSql.Data.Utilities;

namespace PrimarSql.Data.Models.Conditions
{
    internal sealed class MultipleCondition : ICondition
    {
        public bool IsActivated => true;

        public IEnumerable<ICondition> Conditions { get; }

        public MultipleCondition(IEnumerable<ICondition> conditions)
        {
            Conditions = conditions;
        }

        public string ToExpression(AttributeValueManager valueManager)
        {
            return $"({string.Join(", ", Conditions.Where(condition => condition.IsActivated).Select(condition => condition.ToExpression(valueManager)))})";
        }
    }
}
