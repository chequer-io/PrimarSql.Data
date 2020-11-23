using System.Data.Common;
using PrimarSql.Data.Models;

namespace PrimarSql.Data.Planners
{
    internal abstract class QueryPlanner<T> : IQueryPlanner where T : IQueryInfo
    {
        IQueryInfo IQueryPlanner.QueryInfo => QueryInfo;

        public T QueryInfo { get; set; }
        
        public QueryContext QueryContext { get; set; }
        
        public abstract DbDataReader Execute();
    }
}
