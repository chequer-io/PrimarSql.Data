using System.Data.Common;
using PrimarSql.Data.Providers;
using PrimarSql.Data.Models;

namespace PrimarSql.Data.Planners
{
    internal sealed class SelectQueryPlanner : QueryPlanner
    {
        public SelectQueryInfo QueryInfo { get; set; }

        public SelectQueryPlanner(SelectQueryInfo queryInfo)
        {
            QueryInfo = queryInfo;
        }
        
        public override DbDataReader Execute()
        {
            return new PrimarSqlDataReader(
                new ApiDataProvider(QueryContext, QueryInfo)
            );
        }
    }
}
