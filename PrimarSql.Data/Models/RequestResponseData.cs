using System.Collections.Generic;
using Amazon.DynamoDBv2.Model;

namespace PrimarSql.Data.Models
{
    internal sealed class RequestResponseData
    {
        public IEnumerable<Dictionary<string, AttributeValue>> Items { get; set; }
        
        public Dictionary<string, AttributeValue> ExclusiveStartKey { get; set; }
    }
}
