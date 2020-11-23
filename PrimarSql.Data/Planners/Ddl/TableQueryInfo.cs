using PrimarSql.Data.Planners.Table;

namespace PrimarSql.Data.Planners
{
    internal abstract class TableQueryInfo : IQueryInfo
    {
        public string TableName { get; set; }
        
        public TableBillingMode? TableBillingMode { get; set; }
        
        public int? ReadCapacity { get; set; }

        public int? WriteCapacity { get; set; }
    }
}
