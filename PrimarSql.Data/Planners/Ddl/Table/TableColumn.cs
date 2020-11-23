namespace PrimarSql.Data.Planners.Table
{
    internal sealed class TableColumn
    {
        public string ColumnName { get; set; }
        
        public string DataType { get; set; }
        
        public bool IsHashKey { get; set; }
        
        public bool IsSortKey { get; set; }
    }
}
