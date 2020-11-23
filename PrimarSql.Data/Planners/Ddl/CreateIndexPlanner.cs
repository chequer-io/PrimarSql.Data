using System;
using System.Data.Common;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Planners.Index;
using PrimarSql.Data.Providers;

namespace PrimarSql.Data.Planners
{
    internal sealed class CreateIndexPlanner : QueryPlanner<CreateIndexQueryInfo>
    {
        public override DbDataReader Execute()
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

                action.Action(request.GlobalSecondaryIndexUpdates, QueryContext.GetTableDescription(QueryInfo.TableName));

                request.AttributeDefinitions.Add(
                    new AttributeDefinition(indexDefinition.HashKey, DataTypeToScalarAttributeType(QueryInfo.IndexDefinitionWithType.HashKeyType))
                );

                if (!string.IsNullOrEmpty(indexDefinition.SortKey))
                {
                    request.AttributeDefinitions.Add(
                        new AttributeDefinition(indexDefinition.SortKey, DataTypeToScalarAttributeType(QueryInfo.IndexDefinitionWithType.SortKeyType))
                    );
                }
                
                QueryContext.Client.UpdateTableAsync(request).Wait();
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
