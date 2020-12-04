using System;

namespace PrimarSql.Data.Models.Columns
{
    internal sealed class CountFunctionColumn : IColumn
    {
        public IPart[] Name => Array.Empty<IPart>();

        public string Alias { get; set; }
    }
}
