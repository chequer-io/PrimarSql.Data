using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Extensions;
using PrimarSql.Data.Planners.Index;
using PrimarSql.Data.Providers;

namespace PrimarSql.Data.Planners
{
    internal sealed class CreateIndexPlanner : QueryPlanner<CreateIndexQueryInfo>
    {
        public override DbDataReader Execute()
        {
            return ExecuteAsync().GetResultSynchronously();
        }

        public override async Task<DbDataReader> ExecuteAsync(CancellationToken cancellationToken = default)
        {
            var indexDefinition = QueryInfo.IndexDefinitionWithType.IndexDefinition;

            try
            {
                var request = new UpdateTableRequest
                {
                    TableName = QueryInfo.TableName
                };

                var action = new AddIndexAction
                {
                    IndexDefinition = indexDefinition
                };

                action.Action(request.GlobalSecondaryIndexUpdates, await Context.GetTableDescriptionAsync(QueryInfo.TableName, cancellationToken));

                request.AttributeDefinitions.Add(
                    new AttributeDefinition(indexDefinition.HashKey, DataTypeToScalarAttributeType(QueryInfo.IndexDefinitionWithType.HashKeyType))
                );

                if (!string.IsNullOrEmpty(indexDefinition.SortKey))
                {
                    request.AttributeDefinitions.Add(
                        new AttributeDefinition(indexDefinition.SortKey, DataTypeToScalarAttributeType(QueryInfo.IndexDefinitionWithType.SortKeyType))
                    );
                }

                var response = await Context.Client.UpdateTableAsync(request, cancellationToken);

                if (response.TableDescription.TableStatus == TableStatus.UPDATING)
                    await Context.Client.WaitForTableUpdatingAsync(QueryInfo.TableName, cancellationToken);
            }
            catch (Exception e) when (e is not PrimarSqlException)
            {
                throw new PrimarSqlException(
                    PrimarSqlError.Unknown,
                    $"Error while create index '{QueryInfo.TableName}.{indexDefinition.IndexName}'{Environment.NewLine}{e.Message}"
                );
            }

            return new PrimarSqlDataReader(new EmptyDataProvider());
        }
    }
}
