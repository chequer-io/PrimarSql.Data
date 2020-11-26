using PrimarSql.Data.Models;
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
                return new SubqueryProvider(context, queryInfo);
            }

            return new ApiDataProvider(context, queryInfo);
        }
    }
}
