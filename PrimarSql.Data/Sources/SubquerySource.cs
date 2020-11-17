using PrimarSql.Data.Models;

namespace PrimarSql.Data.Sources
{
    public class SubquerySource : ITableSource
    {
        public SelectQueryInfo SubqueryInfo { get; set; }
    }
}
