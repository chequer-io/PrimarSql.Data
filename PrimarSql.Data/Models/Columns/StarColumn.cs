using System;

namespace PrimarSql.Data.Models.Columns
{
    internal sealed class StarColumn : IColumn
    {
        public IPart[] Name { get; } = null;

        public string Alias { get; } = string.Empty;
    }
}
