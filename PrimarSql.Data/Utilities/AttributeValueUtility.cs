using System.Collections.Generic;
using System.Linq;
using Amazon.DynamoDBv2.Model;
using Newtonsoft.Json.Linq;
using PrimarSql.Data.Models;

namespace PrimarSql.Data.Utilities
{
    internal static class AttributeValueUtility
    {
        
        public static JObject ToJObject(this Dictionary<string, AttributeValue> item)
        {
            return new JObject(item.Select(ToJProperty));
        }

        public static JProperty ToJProperty(KeyValuePair<string, AttributeValue> kv)
        {
            var (key, value) = kv;
            
            return new JProperty(key, value.ToJToken());
        }
        
        // TODO: B(Binary), BS(Binary List) Test
        public static JToken ToJToken(this AttributeValue value)
        {
            if (value.NULL)
                return null;
            
            if (value.B != null)
                return new JValue(value.B.ToArray());
            
            if (value.IsLSet)
                return new JArray(value.L.Select(ToJToken));

            if (value.IsMSet)
                return ToJObject(value.M);
            
            if (value.IsBOOLSet)
                return new JValue(value.BOOL);

            if (value.SS.Count > 0)
                return new JArray(value.SS);

            if (value.NS.Count > 0)
                return new JArray(value.NS.Select(n => new JDynamoDBNumber(n)));
            
            if (!string.IsNullOrEmpty(value.N))
                return new JDynamoDBNumber(value.N);

            return value.S;
        }
    }
}
