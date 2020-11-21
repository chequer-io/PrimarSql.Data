using System.Collections.Generic;
using Amazon.DynamoDBv2.Model;

namespace PrimarSql.Data.Models
{
    public class RequestResponseData
    {
        public List<Dictionary<string, AttributeValue>> Items { get; set; }
        
        public Dictionary<string, AttributeValue> ExclusiveStartKey { get; set; }
    }
}
