using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Planners.Table;
using PrimarSql.Data.Providers;

namespace PrimarSql.Data.Planners
{
    internal sealed class AlterTablePlanner : QueryPlanner<AlterTableQueryInfo>
    {
        public override DbDataReader Execute()
        {
            try
            {
                return ExecuteAsync().Result;
            }
            catch (AggregateException e) when (e.InnerExceptions.Count == 1)
            {
                throw e.InnerExceptions[0];
            }
        }

        public override async Task<DbDataReader> ExecuteAsync(CancellationToken cancellationToken = default)
        {
            var tableDescription = await Context.GetTableDescriptionAsync(QueryInfo.TableName, cancellationToken);

            var request = new UpdateTableRequest
            {
                TableName = QueryInfo.TableName
            };

            if (QueryInfo.TableBillingMode != null)
                request.BillingMode = QueryInfo.TableBillingMode switch
                {
                    TableBillingMode.Provisoned => BillingMode.PROVISIONED,
                    TableBillingMode.PayPerRequest => BillingMode.PAY_PER_REQUEST,
                    _ => BillingMode.PROVISIONED
                };

            if (QueryInfo.ReadCapacity != null && QueryInfo.WriteCapacity != null)
                request.ProvisionedThroughput = new ProvisionedThroughput(QueryInfo.ReadCapacity.Value, QueryInfo.WriteCapacity.Value);

            foreach (var action in QueryInfo.IndexActions)
                action.Action(request.GlobalSecondaryIndexUpdates, tableDescription);

            foreach (var column in QueryInfo.TableColumns)
                request.AttributeDefinitions.Add(new AttributeDefinition(column.ColumnName, DataTypeToScalarAttributeType(column.DataType)));

            await Context.Client.UpdateTableAsync(request, cancellationToken);

            return new PrimarSqlDataReader(new EmptyDataProvider());
        }
    }
}
