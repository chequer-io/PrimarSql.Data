﻿using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
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
        private int _deletedCount;

        public DeletePlanner(DeleteQueryInfo queryInfo) : base(queryInfo)
        {
        }

        public async Task<IColumn[]> GetColumnsAsync(CancellationToken cancellationToken = default)
        {
            var columns = new List<IColumn>();

            var tableDescription = await Context.GetTableDescriptionAsync(QueryInfo.TableName, cancellationToken);
            var hashKeyName = tableDescription.KeySchema.First(schema => schema.KeyType == KeyType.HASH).AttributeName;
            var sortKeyName = tableDescription.KeySchema.FirstOrDefault(schema => schema.KeyType == KeyType.RANGE)?.AttributeName;

            columns.Add(new PropertyColumn(hashKeyName));

            if (!string.IsNullOrEmpty(sortKeyName))
                columns.Add(new PropertyColumn(sortKeyName));

            return columns.ToArray();
        }

        public override DbDataReader Execute()
        {
            return ExecuteAsync().GetResultSynchronously();
        }

        public override async Task<DbDataReader> ExecuteAsync(CancellationToken cancellationToken = default)
        {
            _deletedCount = 0;

            var selectQueryInfo = new SelectQueryInfo
            {
                WhereExpression = QueryInfo.WhereExpression,
                Limit = QueryInfo.Limit,
                TableSource = new AtomTableSource(QueryInfo.TableName, string.Empty),
                Columns = await GetColumnsAsync(cancellationToken),
            };

            // TODO: Performance issue, need to performance enhancement.
            if (QueryInfo.WhereExpression == null)
                throw new PrimarSqlException(PrimarSqlError.Syntax, "Update not support without where expression.");

            var planner = new SelectPlanner
            {
                QueryInfo = selectQueryInfo,
                Context = Context
            };

            var reader = await planner.ExecuteAsync(cancellationToken);
            bool hasSortKey = selectQueryInfo.Columns.Length == 2;

            while (await reader.ReadAsync(cancellationToken))
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
                    await Context.Client.DeleteItemAsync(request, cancellationToken);
                }
                catch (Exception e) when (e is not PrimarSqlException)
                {
                    throw new PrimarSqlException(
                        PrimarSqlError.Unknown,
                        $"Error while delete Item (Key: {reader.GetName(0)}){Environment.NewLine}{e.Message}"
                    );
                }

                _deletedCount++;
            }

            return new PrimarSqlDataReader(new EmptyDataProvider(_deletedCount));
        }
    }
}
