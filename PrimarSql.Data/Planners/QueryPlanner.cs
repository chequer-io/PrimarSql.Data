using System.Collections.Generic;

namespace PrimarSql.Data.Planners
{
    // TODO: internal
    public abstract class QueryPlanner
    {
        public List<string> Tables { get; }
        
        public QueryPlanner()
        {
            Tables = new List<string>();
        }
    }
}
