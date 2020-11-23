using System.Data.Common;
using PrimarSql.Data.Providers;

namespace PrimarSql.Data.Planners
{
    internal sealed class SelectQueryPlanner : QueryPlanner<SelectQueryInfo>
    {
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
