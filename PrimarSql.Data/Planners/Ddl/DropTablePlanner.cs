using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using PrimarSql.Data.Providers;

namespace PrimarSql.Data.Planners
{
    internal sealed class DropTablePlanner : QueryPlanner<DropTableQueryInfo>
    {
        public override DbDataReader Execute()
        {
            // TODO: AggregateException!
            return ExecuteAsync().Result;
        }

        public override async Task<DbDataReader> ExecuteAsync(CancellationToken cancellationToken = default)
        {
            foreach (string targetTable in QueryInfo.TargetTables)
            {
                try
                {
                    await Context.Client.DeleteTableAsync(targetTable, cancellationToken);
                }
                catch (AggregateException e)
                {
                    var innerException = e.InnerExceptions[0];
                    throw new Exception($"Error while drop table '{targetTable}'{Environment.NewLine}{innerException.Message}");
                }
            }

            return new PrimarSqlDataReader(new EmptyDataProvider());
        }
    }
}
