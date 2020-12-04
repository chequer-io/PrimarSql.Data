using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Extensions;
using PrimarSql.Data.Models.Columns;
using PrimarSql.Data.Providers;
using PrimarSql.Data.Sources;
using PrimarSql.Data.Utilities;

namespace PrimarSql.Data.Planners
{
    internal sealed class DeletePlanner : QueryPlanner<DeleteQueryInfo>
    {
        private int _deletedCount = 0;
        
        public DeletePlanner(DeleteQueryInfo queryInfo) : base(queryInfo)
        {
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
            
            while (reader.Read())
            {
                var request = new DeleteItemRequest
                {
                    TableName = QueryInfo.TableName,
                    ExpressionAttributeNames = ExpressionAttributeNames,
                    ExpressionAttributeValues = ExpressionAttributeValues
                };

                request.Key.Add(IdentifierUtility.Unescape(reader.GetName(0)), reader[0].ToAttributeValue());

                if (hasSortKey)
                    request.Key.Add(IdentifierUtility.Unescape(reader.GetName(1)), reader[1].ToAttributeValue());

                try
                {
                    Context.Client.DeleteItemAsync(request).Wait();
                }
                catch (AggregateException e)
                {
                    var innerException = e.InnerExceptions[0];
                    throw new Exception($"Error while delete Item (Key: {reader.GetName(0)}){Environment.NewLine}{innerException.Message}");
                }
                
                _deletedCount++;
            }

            return new PrimarSqlDataReader(new EmptyDataProvider(_deletedCount));
        }
    }
}
