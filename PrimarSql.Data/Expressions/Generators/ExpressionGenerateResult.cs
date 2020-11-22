using PrimarSql.Data.Models;

namespace PrimarSql.Data.Expressions.Generators
{
    internal sealed class ExpressionGenerateResult
    {
        public HashKey HashKey { get; set; }
        
        public SortKey SortKey { get; set; }
        
        public ExpressionAttributeName[] ExpressionAttributeNames { get; set; }
        
        public ExpressionAttributeValue[] ExpressionAttributeValues { get; set; }
        
        public string FilterExpression { get; set; }
    }
}
