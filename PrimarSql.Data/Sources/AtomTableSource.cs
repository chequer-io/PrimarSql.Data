namespace PrimarSql.Data.Sources
{
    public class AtomTableSource : ITableSource
    {
        public string TableName { get; set; }

        public AtomTableSource()
        {
            
        }
        
        public AtomTableSource(string tableName)
        {
            TableName = tableName;
        }
    }
}
