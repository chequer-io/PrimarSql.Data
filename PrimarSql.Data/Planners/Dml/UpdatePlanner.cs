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
using PrimarSql.Data.Models.Columns;
using PrimarSql.Data.Providers;
using PrimarSql.Data.Sources;

namespace PrimarSql.Data.Planners
{
    internal sealed class UpdatePlanner : QueryPlanner<UpdateQueryInfo>
    {
        private readonly Dictionary<string, string> _expressionAttributeNames;
        private readonly Dictionary<string, AttributeValue> _expressionAttributeValues;
        private readonly ExpressionAttributeGenerator _attributeGenerator;

        public UpdatePlanner(UpdateQueryInfo queryInfo)
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

        public IColumn[] GetColumns()
        {
            var columns = new List<IColumn>();

            var tableDescription = Context.GetTableDescription(QueryInfo.TableName);
            var hashKeyName = tableDescription.KeySchema.First(schema => schema.KeyType == KeyType.HASH).AttributeName;
            var sortKeyName = tableDescription.KeySchema.FirstOrDefault(schema => schema.KeyType == KeyType.RANGE)?.AttributeName;

            columns.Add(new PropertyColumn(hashKeyName));

            if (!string.IsNullOrEmpty(sortKeyName))
                columns.Add(new PropertyColumn(sortKeyName));

            return columns.ToArray();
        }

        private string GetUpdateExpression()
        {
            var sb = new StringBuilder("SET");
            
            foreach ((IPart[] parts, var expression) in QueryInfo.UpdatedElements)
            {
                // TODO: Support function expression for UpdateExpression
                if (!(expression is LiteralExpression literalExpression))
                    throw new InvalidOperationException($"Not Supported '{expression.GetType().Name}'.");
                    
                var name = GetAttributeName(parts.ToName());
                var value = GetAttributeValue(literalExpression.Value.ToAttributeValue());

                sb.Append($" {name} = {value}");
            }

            return sb.ToString();
        }
        
        public override DbDataReader Execute()
        {
            var selectQueryInfo = new SelectQueryInfo
            {
                WhereExpression = QueryInfo.WhereExpression,
                Limit = QueryInfo.Limit,
                TableSource = new AtomTableSource(QueryInfo.TableName, string.Empty),
                Columns = GetColumns(),
            };

            var planner = new SelectPlanner
            {
                QueryInfo = selectQueryInfo,
                Context = Context
            };

            var reader = planner.Execute();
            bool hasSortKey = selectQueryInfo.Columns.Length == 2;
            string updateExpression = GetUpdateExpression();
            
            while (reader.Read())
            {
                var request = new UpdateItemRequest
                {
                    TableName = QueryInfo.TableName,
                    UpdateExpression = updateExpression,
                    ExpressionAttributeNames = _expressionAttributeNames,
                    ExpressionAttributeValues = _expressionAttributeValues
                };

                request.Key.Add(reader.GetName(0), reader[0].ToAttributeValue());

                if (hasSortKey)
                    request.Key.Add(reader.GetName(1), reader[1].ToAttributeValue());

                try
                {
                    Context.Client.UpdateItemAsync(request).Wait();
                }
                catch (AggregateException e)
                {
                    var innerException = e.InnerExceptions[0];
                    throw new Exception($"Error while Update Item (Key: {reader.GetName(0)}){Environment.NewLine}{innerException.Message}");
                }
            }

            return new PrimarSqlDataReader(new EmptyDataProvider());
        }
    }
}
