namespace PrimarSql.Data.Planners.Index
{
    internal sealed class IndexDefinition
    {
        public bool IsLocalIndex { get; set; }
        
        public string IndexName { get; set; }

        public IndexType IndexType { get; set; }
        
        public string[] IncludeColumns { get; set; }
        
        public string HashKey { get; set; }
        
        public string SortKey { get; set; }
    }
}
