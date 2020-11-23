namespace PrimarSql.Data.Planners.Index
{
    internal sealed class IndexDefinitionWithType
    {
        public IndexDefinition IndexDefinition { get; set; }
        
        public string SortKeyType { get; set; }
        
        public string HashKeyType { get; set; }
    }
}
