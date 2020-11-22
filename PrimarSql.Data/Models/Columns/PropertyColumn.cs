namespace PrimarSql.Data.Models.Columns
{
    internal sealed class PropertyColumn : IColumn
    {
        public IPart[] Name { get; set; }
        
        public string Alias { get; set; }
    }
}
