using System;
using PrimarSql.Data.Models.Columns;

namespace PrimarSql.Data.Utilities
{
    internal static class ValidateUtility
    {
        public static void ValidateSingleName(IPart[] values, string nameType)
        {
            if (values.Length == 0)
                throw new PrimarSqlException(PrimarSqlError.Syntax, $"Empty {nameType} name.");

            if (values.Length > 1)
                throw new PrimarSqlException(PrimarSqlError.Syntax, $"{nameType} name must single identifier");
        }

        public static void ValidateTableWithIndexName(IPart[] values)
        {
            if (values.Length == 0)
                throw new PrimarSqlException(PrimarSqlError.Syntax, "Empty Table name.");

            if (values.Length > 2)
                throw new PrimarSqlException(PrimarSqlError.Syntax, "Identifiers is too long. (table_name[.index_name])");
        }
    }
}
