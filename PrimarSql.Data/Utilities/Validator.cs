using System;

namespace PrimarSql.Data.Utilities
{
    public static class Validator
    {
        public static void ValidateTableName(string[] values)
        {
            if (values.Length == 0)
                throw new InvalidOperationException("Empty Table name.");

            if (values.Length > 1)
                throw new InvalidOperationException("Table name should be single identifier");
        }
    }
}
