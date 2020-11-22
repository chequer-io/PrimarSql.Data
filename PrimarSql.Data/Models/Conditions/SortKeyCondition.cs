using PrimarSql.Data.Utilities;

namespace PrimarSql.Data.Models.Conditions
{
    internal sealed class SortKeyCondition : ICondition
    {
        public SortKey SortKey { get; }

        public bool IsActivated { get; set; } = true;

        public SortKeyCondition(SortKey sortKey)
        {
            SortKey = sortKey;
        }
        
        public string ToExpression(AttributeValueManager valueManager)
        {
            return IsActivated ? SortKey.ToString() : string.Empty;
        }
    }
}
