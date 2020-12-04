using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Expressions;
using PrimarSql.Data.Extensions;
using PrimarSql.Data.Models.Columns;
using PrimarSql.Data.Providers;
using PrimarSql.Data.Sources;
using PrimarSql.Data.Utilities;

namespace PrimarSql.Data.Planners
{
    internal sealed class UpdatePlanner : QueryPlanner<UpdateQueryInfo>
    {
        private int _updatedCount = 0;
        
        public UpdatePlanner(UpdateQueryInfo queryInfo)
        {
            QueryInfo = queryInfo;
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
                    ExpressionAttributeNames = ExpressionAttributeNames,
                    ExpressionAttributeValues = ExpressionAttributeValues
                };

                request.Key.Add(IdentifierUtility.Unescape(reader.GetName(0)), reader[0].ToAttributeValue());

                if (hasSortKey)
                    request.Key.Add(IdentifierUtility.Unescape(reader.GetName(1)), reader[1].ToAttributeValue());

                try
                {
                    Context.Client.UpdateItemAsync(request).Wait();
                }
                catch (AggregateException e)
                {
                    var innerException = e.InnerExceptions[0];
                    throw new Exception($"Error while update Item (Key: {reader.GetName(0)}){Environment.NewLine}{innerException.Message}");
                }

                _updatedCount++;
            }

            return new PrimarSqlDataReader(new EmptyDataProvider(_updatedCount));
        }
    }
}
