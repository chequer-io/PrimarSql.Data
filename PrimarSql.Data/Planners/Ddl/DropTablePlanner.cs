using System;
using System.Data.Common;
using PrimarSql.Data.Providers;

namespace PrimarSql.Data.Planners
{
    internal sealed class DropTablePlanner : QueryPlanner<DropTableQueryInfo>
    {
        public override DbDataReader Execute()
        {
            foreach (string targetTable in QueryInfo.TargetTables)
            {
                try
                {
                    QueryContext.Client.DeleteTableAsync(targetTable).Wait();
                }
                catch (AggregateException e)
                {
                    var innerException = e.InnerExceptions[0];
                    throw new Exception($"Error while Drop table '{targetTable}'{Environment.NewLine}{innerException.Message}");
                }
            }

            return new PrimarSqlDataReader(new EmptyDataProvider());
        }
    }
}
