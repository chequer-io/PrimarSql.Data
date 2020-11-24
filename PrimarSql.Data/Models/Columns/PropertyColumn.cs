namespace PrimarSql.Data.Models.Columns
{
    internal sealed class PropertyColumn : IColumn
    {
        public IPart[] Name { get; set; }

        public string Alias { get; set; }

        public PropertyColumn()
        {
        }

        public PropertyColumn(string name, string alias = "")
        {
            Name = new IPart[] { new IdentifierPart(name) };
            Alias = alias;
        }
    }
}
