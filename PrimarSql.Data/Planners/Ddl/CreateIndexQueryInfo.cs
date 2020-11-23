using PrimarSql.Data.Planners.Index;

namespace PrimarSql.Data.Planners
{
    internal sealed class CreateIndexQueryInfo : IQueryInfo
    {
        public IndexDefinitionWithType IndexDefinitionWithType { get; set; }
        
        public string TableName { get; set; }
    }
}
