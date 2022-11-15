using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PrimarSql.Data.Extensions;
using PrimarSql.Data.Models;
using PrimarSql.Data.Models.Columns;
using PrimarSql.Data.Planners;
using PrimarSql.Data.Sources;

namespace PrimarSql.Data.Providers
{
    internal static class DataProviderFactory
    {
        public static IDataProvider Create(QueryContext context, SelectQueryInfo queryInfo)
        {
            if (queryInfo.TableSource is AtomTableSource)
            {
                var provider = new ApiDataProvider(context, queryInfo);
                provider.Command = context.Command;
                provider.InitializeAsync().WaitSynchronously();

                return provider;
            }

            // TODO: If column contains derived column (expression column is not supported yet)
            if (queryInfo.Columns.Any(column => column is PropertyColumn || column is StarColumn))
            {
                throw new PrimarSqlException(PrimarSqlError.Syntax, "Cannot provide data. Source is not defined.");
            }

            return new EmptyDataProvider();
        }

        public static async Task<IDataProvider> CreateAsync(QueryContext context, SelectQueryInfo queryInfo, CancellationToken cancellationToken)
        {
            if (queryInfo.TableSource is AtomTableSource)
            {
                var provider = new ApiDataProvider(context, queryInfo);
                provider.Command = context.Command;
                await provider.InitializeAsync(cancellationToken);

                return provider;
            }

            // TODO: If column contains derived column (expression column is not supported yet)
            if (queryInfo.Columns.Any(column => column is PropertyColumn || column is StarColumn))
            {
                throw new PrimarSqlException(PrimarSqlError.Syntax, "Cannot provide data. Source is not defined.");
            }

            return new EmptyDataProvider();
        }
    }
}
