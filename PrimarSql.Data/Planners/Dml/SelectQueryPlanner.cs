using System.Data.Common;
using PrimarSql.Data.Cursor;
using PrimarSql.Data.Models;

namespace PrimarSql.Data.Planners
{
    public class SelectQueryPlanner : QueryPlanner
    {
        public SelectQueryInfo QueryInfo { get; set; }

        public SelectQueryPlanner(SelectQueryInfo queryInfo)
        {
            QueryInfo = queryInfo;
        }
        
        public override DbDataReader Execute()
        {
            return new PrimarSqlDataReader(
                new SelectCursor(QueryContext, QueryInfo)
            );
        }
    }
}
