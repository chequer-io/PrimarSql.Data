using PrimarSql.Data.Models;

namespace PrimarSql.Data.Sources
{
    internal sealed class SubquerySource : ITableSource
    {
        public SelectQueryInfo SubqueryInfo { get; set; }
    }
}
