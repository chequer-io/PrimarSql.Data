using System;
using System.Data;
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

            InnerDataProvider = new ApiDataProvider(context, subquerySource.SubqueryInfo);
        }

        public DataTable GetSchemaTable()
        {
            return InnerDataProvider.GetSchemaTable();
        }

        public bool Next()
        {
            return InnerDataProvider.Next();
        }
    }
}
