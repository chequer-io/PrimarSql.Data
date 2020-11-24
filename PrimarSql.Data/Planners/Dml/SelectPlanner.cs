using System.Data.Common;
using PrimarSql.Data.Providers;

namespace PrimarSql.Data.Planners
{
    internal sealed class SelectPlanner : QueryPlanner<SelectQueryInfo>
    {
        public SelectPlanner(SelectQueryInfo queryInfo)
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
