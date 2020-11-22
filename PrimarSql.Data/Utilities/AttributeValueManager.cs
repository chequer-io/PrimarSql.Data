using System.Collections.Generic;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Extensions;

namespace PrimarSql.Data.Utilities
{
    internal sealed class AttributeValueManager
    {
        private const string TrueLiteral = ":literal_true";
        private const string FalseLiteral = ":literal_false";
        private const string ZeroLiteral = ":literal_zero";

        private static readonly Dictionary<string, AttributeValue> LiteralDictionary = new Dictionary<string, AttributeValue>
        {
            { TrueLiteral, true.ToAttributeValue() },
            { FalseLiteral, false.ToAttributeValue() },
            { ZeroLiteral, 0.ToAttributeValue() },
        };

        public Dictionary<string, AttributeValue> AttributeValues { get; }

        public AttributeValueManager()
        {
            AttributeValues = new Dictionary<string, AttributeValue>();
        }

        private string GetLiteralValue(string key)
        {
            if (!AttributeValues.ContainsKey(key))
                AttributeValues[key] = LiteralDictionary[key];

            return key;
        }
        
        public string GetTrueLiteral()
        {
            return GetLiteralValue(TrueLiteral);
        }
          
        public string GetFalseLiteral()
        {
            return GetLiteralValue(FalseLiteral);
        }
          
        public string GetZeroLiteral()
        {
            return GetLiteralValue(ZeroLiteral);
        }
    }
}
