using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Planners.Index;
using PrimarSql.Data.Providers;

namespace PrimarSql.Data.Planners
{
    internal sealed class CreateIndexPlanner : QueryPlanner<CreateIndexQueryInfo>
    {
        public override DbDataReader Execute()
        {
            return ExecuteAsync().Result;
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

                await Context.Client.UpdateTableAsync(request, cancellationToken);
            }
            catch (AggregateException e)
            {
                var innerException = e.InnerExceptions[0];
                throw new Exception($"Error while create index '{QueryInfo.TableName}.{indexDefinition.IndexName}'{Environment.NewLine}{innerException.Message}");
            }

            return new PrimarSqlDataReader(new EmptyDataProvider());
        }
    }
}
