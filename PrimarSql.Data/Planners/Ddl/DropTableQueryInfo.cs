using System.Collections.Generic;

namespace PrimarSql.Data.Planners
{
    internal sealed class DropTableQueryInfo : IQueryInfo
    {
        public IEnumerable<string> TargetTables { get; }

        public DropTableQueryInfo(IEnumerable<string> targetTables)
        {
            TargetTables = targetTables;
        }
    }
}
