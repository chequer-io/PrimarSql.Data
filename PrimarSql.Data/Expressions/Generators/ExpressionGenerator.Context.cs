using System.Collections.Generic;
using System.Linq;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Models;
using PrimarSql.Data.Models.ExpressionBuffers;

namespace PrimarSql.Data.Expressions.Generators
{
    internal partial class ExpressionGenerator
    {
        protected class GeneratorContext
        {
            private int _namesCount;
            private int _valuesCount;

            private readonly Dictionary<ExpressionAttributeName, int> _attributeNames;
            private readonly Dictionary<ExpressionAttributeValue, int> _attributeValues;

            public ExpressionAttributeName[] AttributeNames => _attributeNames.Select(i => i.Key).ToArray();
            
            public ExpressionAttributeValue[] AttributeValues => _attributeValues.Select(i => i.Key).ToArray();

            public List<HashKey> HashKeys { get; } = new List<HashKey>();

            public List<SortKey> SortKeys { get; } = new List<SortKey>();
            
            public List<IBuffer> Buffers { get; }

            public int BufferIndex => Buffers.Count - 1;

            internal GeneratorContext()
            {
                _attributeNames = new Dictionary<ExpressionAttributeName, int>();
                _attributeValues = new Dictionary<ExpressionAttributeValue, int>();
                Buffers = new List<IBuffer>();
            }

            public int Append(string expression)
            {
                Buffers.Add(new StringBuffer(expression));

                return Buffers.Count - 1;
            }

            public bool RemoveKey(IKey key)
            {
                if (key is HashKey hashKey && HashKeys.Contains(hashKey))
                {
                    Leave(hashKey.ExpressionAttributeName);
                    Leave(hashKey.ExpressionAttributeValue);

                    return HashKeys.Remove(hashKey);
                }

                if (key is SortKey sortKey && SortKeys.Contains(sortKey))
                {
                    Leave(sortKey.ExpressionAttributeName);
                    Leave(sortKey.ExpressionAttributeValue);
                    Leave(sortKey.ExpressionAttributeValue2);

                    return SortKeys.Remove(sortKey);
                }

                return false;
            }

            public void RemoveLastToken()
            {
                Buffers.RemoveAt(Buffers.Count - 1);
            }

            public string Flush()
            {
                return string.Join(" ", Buffers);
            }

            public HashKey AddHashKey(ExpressionAttributeName attrName, ExpressionAttributeValue attrValue)
            {
                var hashKey = new HashKey
                {
                    ExpressionAttributeName = attrName,
                    ExpressionAttributeValue = attrValue,
                };

                HashKeys.Add(hashKey);

                return hashKey;
            }

            public SortKey AddSortKey(
                ExpressionAttributeName name,
                ExpressionAttributeValue value,
                ExpressionAttributeValue value2,
                string @operator,
                SortKeyType sortKeyType
            )
            {
                var sortKey = new SortKey
                {
                    ExpressionAttributeName = name,
                    ExpressionAttributeValue = value,
                    ExpressionAttributeValue2 = value2,
                    Operator = @operator,
                    SortKeyType = sortKeyType
                };

                SortKeys.Add(sortKey);

                return sortKey;
            }

            public ExpressionAttributeName GetAttributeName(string rawColumnName)
            {
                var attrName = new ExpressionAttributeName($"#name{_namesCount++}", rawColumnName);
                _attributeNames[attrName] = 1;

                return attrName;
            }

            public ExpressionAttributeValue GetAttributeValue(AttributeValue value)
            {
                var attrValue = new ExpressionAttributeValue($":value{_valuesCount++}", value);
                _attributeValues[attrValue] = 1;

                return attrValue;
            }

            public void Enter(ExpressionAttributeName attrName)
            {
                if (_attributeNames.ContainsKey(attrName))
                    _attributeNames[attrName]++;
            }

            public void Enter(ExpressionAttributeValue attrValue)
            {
                if (_attributeValues.ContainsKey(attrValue))
                    _attributeValues[attrValue]++;
            }

            public void Leave(ExpressionAttributeName attrName)
            {
                if (attrName != null && _attributeNames.ContainsKey(attrName))
                {
                    if (_attributeNames[attrName] == 1)
                    {
                        _attributeNames.Remove(attrName);
                    }
                    else
                    {
                        _attributeNames[attrName]--;
                    }
                }
            }

            public void Leave(ExpressionAttributeValue attrValue)
            {
                if (attrValue != null && _attributeValues.ContainsKey(attrValue))
                {
                    if (_attributeValues[attrValue] == 1)
                    {
                        _attributeValues.Remove(attrValue);
                    }
                    else
                    {
                        _attributeValues[attrValue]--;
                    }
                }
            }
        }
    }
}
