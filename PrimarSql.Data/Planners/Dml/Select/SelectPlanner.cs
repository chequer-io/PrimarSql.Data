using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
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

        public override async Task<DbDataReader> ExecuteAsync(CancellationToken cancellationToken = default)
        {
            var provider = await DataProviderFactory.CreateAsync(Context, QueryInfo, cancellationToken);
            return new PrimarSqlDataReader(provider);
        }
    }
}
