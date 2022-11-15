using System;
using System.Data.Common;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Extensions;
using PrimarSql.Data.Providers;

namespace PrimarSql.Data.Planners
{
    internal sealed class DropTablePlanner : QueryPlanner<DropTableQueryInfo>
    {
        public override DbDataReader Execute()
        {
            return ExecuteAsync().GetResultSynchronously();
        }

        public override async Task<DbDataReader> ExecuteAsync(CancellationToken cancellationToken = default)
        {
            foreach (string targetTable in QueryInfo.TargetTables)
            {
                try
                {
                    await Context.Client.DeleteTableAsync(targetTable, cancellationToken);
                }
                catch (PrimarSqlException e) when (e.Error is PrimarSqlError.ResourceNotFound && QueryInfo.IfExists)
                {
                    // ignored table if not exists
                }
                catch (Exception e) when (e is not PrimarSqlException)
                {
                    throw new PrimarSqlException(
                        PrimarSqlError.Unknown,
                        $"Error while drop table '{targetTable}'{Environment.NewLine}{e.Message}"
                    );
                }
            }

            foreach (var targetTable in QueryInfo.TargetTables)
                await Context.Client.WaitForTableDeletingAsync(targetTable, cancellationToken);

            return new PrimarSqlDataReader(new EmptyDataProvider());
        }
    }
}
