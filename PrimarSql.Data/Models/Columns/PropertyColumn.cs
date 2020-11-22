namespace PrimarSql.Data.Models.Columns
{
    internal sealed class PropertyColumn : IColumn
    {
        public object[] Name { get; set; }
        
        public string Alias { get; set; }
    }
}
