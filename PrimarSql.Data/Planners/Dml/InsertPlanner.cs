using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Expressions;
using PrimarSql.Data.Expressions.Generators;
using PrimarSql.Data.Extensions;
using PrimarSql.Data.Providers;

namespace PrimarSql.Data.Planners
{
    internal sealed class InsertPlanner : QueryPlanner<InsertQueryInfo>
    {
        private readonly Dictionary<string, string> _expressionAttributeNames;
        private readonly Dictionary<string, AttributeValue> _expressionAttributeValues;
        private readonly ExpressionAttributeGenerator _attributeGenerator;
        private string _hashKeyName = string.Empty;
        private string _sortKeyName = string.Empty;
        private AttributeValue _hashKeyValue;
        private AttributeValue _sortKeyValue;
        private PutItemRequest _request;
        private int _insertCount;
        
        public InsertPlanner(InsertQueryInfo queryInfo)
        {
            _expressionAttributeNames = new Dictionary<string, string>();
            _expressionAttributeValues = new Dictionary<string, AttributeValue>();
            _attributeGenerator = new ExpressionAttributeGenerator();
            QueryInfo = queryInfo;
        }

        private string GetAttributeName(string rawName)
        {
            var attributeName = _attributeGenerator.GetAttributeName(rawName);
            _expressionAttributeNames[attributeName.Key] = attributeName.Value;

            return attributeName.Key;
        }
        
        private string GetAttributeValue(AttributeValue rawValue)
        {
            var attributeValue = _attributeGenerator.GetAttributeValue(rawValue);
            _expressionAttributeValues[attributeValue.Key] = attributeValue.Value;

            return attributeValue.Key;
        }
        
        public override DbDataReader Execute()
        {
            var tableDescription = Context.GetTableDescription(QueryInfo.TableName);
            List<KeySchemaElement> keySchema = tableDescription.KeySchema;
            _hashKeyName = keySchema.First(schema => schema.KeyType == KeyType.HASH).AttributeName;
            _sortKeyName = keySchema.FirstOrDefault(schema => schema.KeyType == KeyType.RANGE)?.AttributeName;

            if (QueryInfo.FromSelectQuery)
                throw new NotSupportedException("Insert from select feature is not supported yet.");

            if (QueryInfo.Columns.Length == 0)
                QueryInfo.Columns = keySchema.Select(schema => schema.AttributeName).ToArray();

            if (!QueryInfo.Columns.Contains(_hashKeyName))
                throw new NotSupportedException("The value must contain the hash key.");

            foreach (IEnumerable<IExpression> row in QueryInfo.Rows)
            {
                PutItem(row);
                _insertCount++;
            }

            return new PrimarSqlDataReader(new EmptyDataProvider());
        }
        
        private void SetConditionalExpression()
        {
            var sb = new StringBuilder();

            var hashKeyName = GetAttributeName(_hashKeyName);
            var hashKeyValue = GetAttributeValue(_hashKeyValue);

            sb.Append($"{hashKeyName} <> {hashKeyValue}");

            if (!string.IsNullOrEmpty(_sortKeyName))
            {
                var sortKeyName = GetAttributeName(_sortKeyName);
                var sortKeyValue = GetAttributeValue(_sortKeyValue);

                sb.Append($" AND {sortKeyName} <> {sortKeyValue}");
            }

            _request.ExpressionAttributeNames = _expressionAttributeNames;
            _request.ExpressionAttributeValues = _expressionAttributeValues;
            _request.ConditionExpression = sb.ToString();
        }

        private void PutItem(IEnumerable<IExpression> row)
        {
            _expressionAttributeNames.Clear();
            _expressionAttributeNames.Clear();
            _attributeGenerator.ResetIndex();
            _hashKeyValue = null;
            _sortKeyName = null;
            _request = null;
            
            var item = new Dictionary<string, AttributeValue>();
            int i = 0;

            foreach (var cell in row)
            {
                if (i + 1 > QueryInfo.Columns.Length)
                    throw new InvalidOperationException("The number of values cannot exceed the number of columns.");

                if (cell is LiteralExpression literalExpression)
                {
                    string column = QueryInfo.Columns[i];
                    var attrValue = literalExpression.Value.ToAttributeValue();

                    item[column] = attrValue;

                    if (column == _hashKeyName)
                        _hashKeyValue = attrValue;
                    else if (column == _hashKeyName)
                        _sortKeyValue = attrValue;
                }
                else
                {
                    throw new NotSupportedException($"Not Support '{cell.GetType().Name}' type to insert value.");
                }

                i++;
            }

            if (item.Count != QueryInfo.Columns.Length)
                throw new InvalidOperationException("The number of values does not match the number of columns.");

            _request = new PutItemRequest
            {
                TableName = QueryInfo.TableName,
                Item = item
            };

            SetConditionalExpression();

            try
            {
                Context.Client.PutItemAsync(_request).Wait();
            }
            catch (AggregateException e)
            {
                var innerException = e.InnerExceptions[0];

                if (innerException is ConditionalCheckFailedException && QueryInfo.IgnoreDuplicate)
                    return;
                
                throw new Exception($"Error while insert item (Count: {_insertCount}){Environment.NewLine}{innerException.Message}");
            }
        }
    }
}
