using System;
using System.Linq;
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
            if (queryInfo.TableSource is SubquerySource)
            {
                throw new NotSupportedException("Subquery feature is not supported yet.");
                // return new SubqueryProvider(context, queryInfo);
            }

            if (queryInfo.TableSource is AtomTableSource)
            {
                return new ApiDataProvider(context, queryInfo);
            }

            // TODO: If column contains derived column (expression column is not supported yet)
            if (queryInfo.Columns.Any(column => column is PropertyColumn || column is StarColumn))
            {
                throw new InvalidOperationException("Cannot provide data. Source is not defined.");
            }
            
            return new EmptyDataProvider();
        }
    }
}
