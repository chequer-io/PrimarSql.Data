using System;
using System.Data.Common;

namespace PrimarSql.Data.Planners
{
    public class DropTablePlanner : QueryPlanner
    {
        public string[] TargetTables { get; set; }
        
        public override DbDataReader Execute()
        {
            foreach (string targetTable in TargetTables)
            {
                Console.WriteLine(targetTable);
            }

            return null;
        }
    }
}
