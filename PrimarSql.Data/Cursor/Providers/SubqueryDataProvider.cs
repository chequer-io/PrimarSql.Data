using System;
using PrimarSql.Data.Models;
using PrimarSql.Data.Sources;

namespace PrimarSql.Data.Cursor.Providers
{
    public class SubqueryDataProvider : IDataProvider
    {
        public bool HasRows => InnerDataProvider.HasRows;

        public DataCell[] Current => InnerDataProvider.Current;

        private IDataProvider InnerDataProvider { get; }
        
        public SubqueryDataProvider(QueryContext context, SelectQueryInfo queryInfo)
        {
            if (!(queryInfo.TableSource is SubquerySource subquerySource))
                throw new InvalidOperationException($"SubqueryDataProvider cannot handle {queryInfo.TableSource?.GetType().Name}");

            InnerDataProvider = new TableDataProvider(context, subquerySource.SubqueryInfo);
        }
        
        public bool Next()
        {
            return InnerDataProvider.Next();
        }
    }
}
