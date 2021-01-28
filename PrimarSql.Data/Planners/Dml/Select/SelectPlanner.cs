using System.Data.Common;
using PrimarSql.Data.Providers;

namespace PrimarSql.Data.Planners
{
    internal sealed class SelectPlanner : QueryPlanner<SelectQueryInfo>
    {
        public SelectPlanner()
        {
        }

        public SelectPlanner(SelectQueryInfo queryInfo) : base(queryInfo)
        {
        }

        public override DbDataReader Execute()
        {
            var provider = DataProviderFactory.Create(Context, QueryInfo);

            return new PrimarSqlDataReader(provider);
        }
    }
}
