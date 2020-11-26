namespace PrimarSql.Data.Models.Columns
{
    internal interface IColumn
    {
        IPart[] Name { get; }

        string Alias { get; }
    }
}
