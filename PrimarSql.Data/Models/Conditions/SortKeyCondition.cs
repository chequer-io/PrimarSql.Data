using PrimarSql.Data.Utilities;

namespace PrimarSql.Data.Models.Conditions
{
    public class SortKeyCondition : ICondition
    {
        public SortKey SortKey { get; }
        
        public bool IsActivated { get; set; }

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
