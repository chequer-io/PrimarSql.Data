using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using PrimarSql.Data.Extensions;
using PrimarSql.Data.Providers;

namespace PrimarSql.Data.Planners.Show
{
    internal sealed class ShowTablePlanner : QueryPlanner<EmptyQueryInfo>
    {
        public ShowTablePlanner()
        {
            QueryInfo = new EmptyQueryInfo();
        }

        public override DbDataReader Execute()
        {
            return ExecuteAsync().GetResultSynchronously();
        }

        public override Task<DbDataReader> ExecuteAsync(CancellationToken cancellationToken = default)
        {
            return Task.FromResult<DbDataReader>(new PrimarSqlDataReader(new ShowTableProvider(Context.Client)));
        }
    }
}
