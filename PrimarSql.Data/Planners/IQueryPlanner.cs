using System.Data.Common;
using PrimarSql.Data.Models;

namespace PrimarSql.Data.Planners
{
    internal interface IQueryPlanner
    {
        IQueryInfo QueryInfo { get; }
        
        QueryContext QueryContext { get; set; }
        
        DbDataReader Execute();
    }
}
