using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
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
            try
            {
                return ExecuteAsync().Result;
            }
            catch (AggregateException e) when (e.InnerExceptions.Count == 1)
            {
                throw e.InnerExceptions[0];
            }
        }

        public override Task<DbDataReader> ExecuteAsync(CancellationToken cancellationToken = default)
        {
            return Task.FromResult<DbDataReader>(new PrimarSqlDataReader(new ShowTableProvider(Context.Client)));
        }
    }
}
