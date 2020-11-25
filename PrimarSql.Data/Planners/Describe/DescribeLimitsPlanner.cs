using System.Data.Common;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Providers;

namespace PrimarSql.Data.Planners.Describe
{
    internal sealed class DescribeLimitsPlanner : QueryPlanner<EmptyQueryInfo>
    {
        public override DbDataReader Execute()
        {
            var provider = new ListDataProvider();
            var response = Context.Client.DescribeLimitsAsync(new DescribeLimitsRequest()).Result;

            provider.AddColumn("TableMaxReadCapacityUnits", typeof(long));
            provider.AddColumn("TableMaxWriteCapacityUnits", typeof(long));
            provider.AddColumn("AccountMaxReadCapacityUnits", typeof(long));
            provider.AddColumn("AccountMaxWriteCapacityUnits", typeof(long));

            provider.AddRow(
                response.TableMaxReadCapacityUnits,
                response.TableMaxWriteCapacityUnits,
                response.AccountMaxReadCapacityUnits,
                response.AccountMaxWriteCapacityUnits
            );

            return new PrimarSqlDataReader(provider);
        }
    }
}
