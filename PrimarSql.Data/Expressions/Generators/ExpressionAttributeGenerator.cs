using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Models;

namespace PrimarSql.Data.Expressions.Generators
{
    internal class ExpressionAttributeGenerator
    {
        private int _nameIndex;
        private int _valueIndex;

        public void ResetIndex()
        {
            _nameIndex = 0;
            _valueIndex = 0;
        }
        
        public ExpressionAttributeName GetAttributeName(string rawColumnName)
        {
            return new ExpressionAttributeName($"#name{_nameIndex++}", rawColumnName);
        }

        public ExpressionAttributeValue GetAttributeValue(AttributeValue value)
        {
            return new ExpressionAttributeValue($":value{_valueIndex++}", value);
        }
    }
}
