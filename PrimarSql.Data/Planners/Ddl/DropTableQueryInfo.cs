using System.Collections.Generic;

namespace PrimarSql.Data.Planners
{
    public class DropTableQueryInfo : IQueryInfo
    {
        public IEnumerable<string> TargetTables { get; }

        public DropTableQueryInfo(IEnumerable<string> targetTables)
        {
            TargetTables = targetTables;
        }
    }
}
