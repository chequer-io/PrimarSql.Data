namespace PrimarSql.Data.Planners.Table
{
    internal sealed class KeyTableColumn : TableColumn
    {
        public bool IsHashKey { get; set; }
        
        public bool IsSortKey { get; set; }
    }
}
