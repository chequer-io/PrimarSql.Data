using System.Collections.Generic;
using System.Linq;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Models;
using PrimarSql.Data.Models.Conditions;

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

            public IEnumerable<ExpressionAttributeName> AttributeNames => _attributeNames.Keys;

            public IEnumerable<ExpressionAttributeValue> AttributeValues => _attributeValues.Keys;

            public Dictionary<HashKey, HashKeyCondition> HashKeys { get; } = new Dictionary<HashKey, HashKeyCondition>();

            public Dictionary<SortKey, SortKeyCondition> SortKeys { get; } = new Dictionary<SortKey, SortKeyCondition>();

            internal GeneratorContext()
            {
                _attributeNames = new Dictionary<ExpressionAttributeName, int>();
                _attributeValues = new Dictionary<ExpressionAttributeValue, int>();
            }

            public bool RemoveKey(IKey key)
            {
                if (key is HashKey hashKey && HashKeys.ContainsKey(hashKey))
                {
                    Leave(hashKey.ExpressionAttributeName);
                    Leave(hashKey.ExpressionAttributeValue);

                    return HashKeys.Remove(hashKey);
                }

                if (key is SortKey sortKey && SortKeys.ContainsKey(sortKey))
                {
                    Leave(sortKey.ExpressionAttributeName);
                    Leave(sortKey.ExpressionAttributeValue);
                    Leave(sortKey.ExpressionAttributeValue2);

                    return SortKeys.Remove(sortKey);
                }

                return false;
            }

            public HashKeyCondition AddHashKeyCondition(ExpressionAttributeName attrName, ExpressionAttributeValue attrValue)
            {
                var hashKey = new HashKey
                {
                    ExpressionAttributeName = attrName,
                    ExpressionAttributeValue = attrValue,
                };

                var hashKeyCondition = new HashKeyCondition(hashKey);
                HashKeys.Add(hashKey, hashKeyCondition);

                return hashKeyCondition;
            }

            public SortKeyCondition AddSortKeyCondition(
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

                var sortKeyCondition = new SortKeyCondition(sortKey);
                SortKeys.Add(sortKey, sortKeyCondition);

                return sortKeyCondition;
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
