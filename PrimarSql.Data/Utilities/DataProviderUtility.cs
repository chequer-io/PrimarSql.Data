using System;
using System.Data;
using System.Data.Common;
using PrimarSql.Data.Models.Columns;

namespace PrimarSql.Data.Utilities
{
    internal static class DataProviderUtility
    {
        public static DataTable GetNewSchemaTable()
        {
            var schemaTable = new DataTable();

            schemaTable.Columns.Add(SchemaTableColumn.ColumnName, typeof(string));
            schemaTable.Columns.Add(SchemaTableColumn.ColumnOrdinal, typeof(int));
            schemaTable.Columns.Add(SchemaTableColumn.DataType, typeof(Type));
            schemaTable.Columns.Add("Path", typeof(IPart[]));
            schemaTable.Columns.Add(SchemaTableOptionalColumn.IsReadOnly, typeof(bool));

            return schemaTable;
        }
    }
}
