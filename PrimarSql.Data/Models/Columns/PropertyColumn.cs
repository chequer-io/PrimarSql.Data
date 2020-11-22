namespace PrimarSql.Data.Models.Columns
{
    public class PropertyColumn : IColumn
    {
        public object[] Name { get; set; }
        
        public string Alias { get; set; }
    }
}
