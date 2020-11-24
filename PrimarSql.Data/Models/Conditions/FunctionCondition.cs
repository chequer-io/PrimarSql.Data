using System.Linq;
using PrimarSql.Data.Utilities;

namespace PrimarSql.Data.Models.Conditions
{
    internal sealed class FunctionCondition : ICondition
    {
        public string MemberName { get; set; }

        public ICondition[] Parameters { get; set; }
        
        public bool IsActivated => true;

        public string ToExpression(AttributeValueManager valueManager)
        {
            return $"{MemberName.ToLower()}({string.Join(", ", Parameters.Where(p => p.IsActivated).Select(p => p.ToExpression(valueManager)))})";
        }
    }
}
