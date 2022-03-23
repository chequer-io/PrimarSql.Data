using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Providers;

namespace PrimarSql.Data.Planners.Describe
{
    internal sealed class DescribeEndPointsPlanner : QueryPlanner<EmptyQueryInfo>
    {
        public override DbDataReader Execute()
        {
            return ExecuteAsync().Result;
        }

        public override async Task<DbDataReader> ExecuteAsync(CancellationToken cancellationToken = default)
        {
            var provider = new ListDataProvider();
            var response = await Context.Client.DescribeEndpointsAsync(new DescribeEndpointsRequest(), cancellationToken);

            provider.AddColumn("Address", typeof(string));
            provider.AddColumn("CachePeriodInMinutes", typeof(long));

            foreach (var endpoint in response.Endpoints)
            {
                provider.AddRow(endpoint.Address, endpoint.CachePeriodInMinutes);
            }

            return new PrimarSqlDataReader(provider);
        }
    }
}
