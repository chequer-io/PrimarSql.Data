using PrimarSql.Data.Utilities;

namespace PrimarSql.Data.Models.Conditions
{
    public class HashKeyCondition : ICondition
    {
        public HashKey HashKey { get; }

        public bool IsActivated { get; set; } = true;

        public string ToExpression(AttributeValueManager valueManager)
        {
            return IsActivated ? HashKey.ToString() : string.Empty;
        }

        public HashKeyCondition(HashKey hashKey)
        {
            HashKey = hashKey;
        }
    }
}
