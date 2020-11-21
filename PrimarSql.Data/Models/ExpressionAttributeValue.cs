using Amazon.DynamoDBv2.Model;

namespace PrimarSql.Data.Models
{
    public class ExpressionAttributeValue
    {
        public string Key { get; }

        public AttributeValue Value { get; }

        public ExpressionAttributeValue(string key, AttributeValue value)
        {
            Key = key;
            Value = value;
        }
        
        public override string ToString()
        {
            return Key;
        }
    }
}
