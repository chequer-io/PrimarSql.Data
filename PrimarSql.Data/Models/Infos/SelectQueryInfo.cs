using PrimarSql.Data.Models.Columns;
using PrimarSql.Data.Sources;

namespace PrimarSql.Data.Models
{
    public sealed class SelectQueryInfo
    {
        public ITableSource TableSource { get; set; }
        
        public IColumn[] Columns { get; set; }
        
        public bool UseStronglyConsistent { get; set; } = false;

        public long Limit { get; set; }

        public long Offset { get; set; }

        public bool OrderDescend { get; set; } = false;
    }
}
