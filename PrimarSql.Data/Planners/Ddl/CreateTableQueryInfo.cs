using PrimarSql.Data.Planners.Index;
using PrimarSql.Data.Planners.Table;

namespace PrimarSql.Data.Planners
{
    internal sealed class CreateTableQueryInfo : IQueryInfo
    {
        public bool SkipIfExists { get; set; }
        
        public string TableName { get; set; }

        public TableColumn[] TableColumns { get; set; }
        
        public TableBillingMode TableBillingMode { get; set; }
        
        public int? ReadCapacity { get; set; }
        
        public int? WriteCapacity { get; set; }
        
        public IndexDefinition[] IndexDefinitions { get; set; }
    }
}
