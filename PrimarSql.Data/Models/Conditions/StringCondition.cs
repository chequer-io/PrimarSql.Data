using PrimarSql.Data.Utilities;

namespace PrimarSql.Data.Models.Conditions
{
    public class StringCondition : ICondition
    {
        public string Value { get; }

        public bool IsActivated => true;

        public StringCondition(string value)
        {
            Value = value;
        }
        
        public string ToExpression(AttributeValueManager valueManager)
        {
            return Value;
        }
    }
}
