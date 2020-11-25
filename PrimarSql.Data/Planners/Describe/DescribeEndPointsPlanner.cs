using System.Data.Common;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Providers;

namespace PrimarSql.Data.Planners.Describe
{
    internal sealed class DescribeEndPointsPlanner : QueryPlanner<EmptyQueryInfo>
    {
        public override DbDataReader Execute()
        {
            var provider = new ListDataProvider();
            var response = Context.Client.DescribeEndpointsAsync(new DescribeEndpointsRequest()).Result;

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
