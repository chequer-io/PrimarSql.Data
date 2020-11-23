using System.Data.Common;
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
            var tableDescription = QueryContext.GetTableDescription(QueryInfo.TableName);
            
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
            
            QueryContext.Client.UpdateTableAsync(request).Wait();

            return new PrimarSqlDataReader(new EmptyDataProvider());
        }
    }
}
