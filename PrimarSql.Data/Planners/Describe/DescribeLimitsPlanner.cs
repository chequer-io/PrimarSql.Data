using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Extensions;
using PrimarSql.Data.Providers;

namespace PrimarSql.Data.Planners.Describe
{
    internal sealed class DescribeLimitsPlanner : QueryPlanner<EmptyQueryInfo>
    {
        public override DbDataReader Execute()
        {
            return ExecuteAsync().GetResultSynchronously();
        }

        public override async Task<DbDataReader> ExecuteAsync(CancellationToken cancellationToken = default)
        {
            var provider = new ListDataProvider();
            var response = await Context.Client.DescribeLimitsAsync(new DescribeLimitsRequest(), cancellationToken);

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
