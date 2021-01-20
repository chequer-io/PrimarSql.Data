using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using Amazon.DynamoDBv2.Model;
using Newtonsoft.Json.Linq;

namespace PrimarSql.Data.Extensions
{
    internal static class AttributeValueExtension
    {
        public static object ToValue(this AttributeValue value)
        {
            if (value.NULL)
                return DBNull.Value;

            if (value.IsLSet)
                return value.L.Select(v => v.ToValue()).ToList();

            if (value.IsMSet)
                return string.Join(", ", value.M);

            if (value.IsBOOLSet)
                return value.BOOL;

            if (value.SS.Count > 0)
                return value.SS;

            if (value.B != null)
                return value.B.ToArray();

            if (!string.IsNullOrEmpty(value.N))
                return double.Parse(value.N);

            return value.S;
        }

        public static AttributeValue ToAttributeValue(this object value)
        {
            return value switch
            {
                double d => d.ToAttributeValue(),
                float f => f.ToAttributeValue(),
                int i => i.ToAttributeValue(),
                uint ui => ui.ToAttributeValue(),
                long l => l.ToAttributeValue(),
                ulong ul => ul.ToAttributeValue(),
                short sh => sh.ToAttributeValue(),
                ushort ush => ush.ToAttributeValue(),
                bool b => b.ToAttributeValue(),
                string s => s.ToAttributeValue(),
                IEnumerable<int> iList => iList.ToAttributeValue(),
                IEnumerable<string> sList => sList.ToAttributeValue(),
                JValue jValue => jValue.ToAttributeValue(),
                JArray jArray => jArray.ToAttributeValue(),
                JObject jObject => jObject.ToAttributeValue(),
                byte[] bytes => bytes.ToAttributeValue(),
                DBNull dbNull => dbNull.ToAttributeValue(),
                IEnumerable<object> oList => oList.ToAttributeValue(),
                _ => GetNullAttributeValue()
            };
        }

        public static AttributeValue GetNullAttributeValue()
        {
            return new AttributeValue
            {
                NULL = true
            };
        }

        public static AttributeValue ToAttributeValue(this double value)
        {
            return new AttributeValue
            {
                N = value.ToString(CultureInfo.CurrentCulture)
            };
        }

        public static AttributeValue ToAttributeValue(this float value)
        {
            return new AttributeValue
            {
                N = value.ToString(CultureInfo.CurrentCulture)
            };
        }

        public static AttributeValue ToAttributeValue(this int value)
        {
            return new AttributeValue
            {
                N = value.ToString()
            };
        }

        public static AttributeValue ToAttributeValue(this uint value)
        {
            return new AttributeValue
            {
                N = value.ToString()
            };
        }

        public static AttributeValue ToAttributeValue(this long value)
        {
            return new AttributeValue
            {
                N = value.ToString()
            };
        }

        public static AttributeValue ToAttributeValue(this ulong value)
        {
            return new AttributeValue
            {
                N = value.ToString()
            };
        }

        public static AttributeValue ToAttributeValue(this short value)
        {
            return new AttributeValue
            {
                N = value.ToString()
            };
        }

        public static AttributeValue ToAttributeValue(this ushort value)
        {
            return new AttributeValue
            {
                N = value.ToString()
            };
        }

        public static AttributeValue ToAttributeValue(this bool value)
        {
            return new AttributeValue
            {
                BOOL = value
            };
        }

        public static AttributeValue ToAttributeValue(this string value)
        {
            return new AttributeValue
            {
                S = value
            };
        }

        public static AttributeValue ToAttributeValue(this IEnumerable<int> values)
        {
            return new AttributeValue
            {
                NS = values.Select(value => value.ToString()).ToList()
            };
        }

        public static AttributeValue ToAttributeValue(this IEnumerable<string> values)
        {
            return new AttributeValue
            {
                SS = values.ToList()
            };
        }

        public static AttributeValue ToAttributeValue(this IEnumerable<AttributeValue> values)
        {
            return new AttributeValue
            {
                L = values.ToList()
            };
        }

        public static AttributeValue ToAttributeValue(this JToken jToken)
        {
            return jToken switch
            {
                JValue jValue => jValue.ToAttributeValue(),
                JArray jArray => jArray.ToAttributeValue(),
                JObject jObject => jObject.ToAttributeValue(),
                _ => GetNullAttributeValue()
            };
        }

        public static AttributeValue ToAttributeValue(this JValue jValue)
        {
            return jValue.Value.ToAttributeValue();
        }

        public static AttributeValue ToAttributeValue(this JArray jArray)
        {
            return new AttributeValue
            {
                L = jArray.Values().Select(ToAttributeValue).ToList()
            };
        }

        public static AttributeValue ToAttributeValue(this JObject jObject)
        {
            return new AttributeValue
            {
                M = jObject
                    .Properties()
                    .Select(property => (property.Name, property.Value.ToAttributeValue()))
                    .ToDictionary(kv => kv.Name, kv => kv.Item2)
            };
        }

        public static AttributeValue ToAttributeValue(this byte[] bytes)
        {
            return new AttributeValue
            {
                B = new MemoryStream(bytes)
            };
        }

        public static AttributeValue ToAttributeValue(this DBNull _)
        {
            return new AttributeValue
            {
                NULL = true
            };
        }
    }
}
