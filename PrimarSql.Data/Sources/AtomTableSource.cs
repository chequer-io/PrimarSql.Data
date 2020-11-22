namespace PrimarSql.Data.Sources
{
    internal sealed class AtomTableSource : ITableSource
    {
        public string TableName { get; set; }

        public string IndexName { get; set; }
        
        public AtomTableSource()
        {
            
        }
        
        public AtomTableSource(string tableName, string indexName)
        {
            TableName = tableName;
            IndexName = indexName;
        }
    }
}
