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
            var request = new UpdateTableRequest();

            request.TableName = QueryInfo.TableName;

            if (QueryInfo.TableBillingMode != null)
                request.BillingMode = QueryInfo.TableBillingMode switch
                {
                    TableBillingMode.Provisoned => BillingMode.PROVISIONED,
                    TableBillingMode.PayPerRequest => BillingMode.PAY_PER_REQUEST,
                    _ => BillingMode.PROVISIONED
                };

            if (QueryInfo.ReadCapacity != null && QueryInfo.WriteCapacity != null)
                request.ProvisionedThroughput = new ProvisionedThroughput(QueryInfo.ReadCapacity.Value, QueryInfo.WriteCapacity.Value);

            // TODO: Add Alter new Column to Grammer and add Attribute to AlterTableQueryInfo
            
            foreach (var action in QueryInfo.IndexActions)
                action.Action(request);

            return new PrimarSqlDataReader(new EmptyDataProvider());
        }
    }
}
