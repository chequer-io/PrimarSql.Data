using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using PrimarSql.Data.Models;

namespace PrimarSql.Data.Planners
{
    internal interface IQueryPlanner
    {
        IQueryInfo QueryInfo { get; }

        QueryContext Context { get; set; }

        DbDataReader Execute();

        Task<DbDataReader> ExecuteAsync(CancellationToken cancellationToken = default);
    }
}
