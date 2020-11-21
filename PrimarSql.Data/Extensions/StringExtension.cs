using System;

namespace PrimarSql.Data.Extensions
{
    public static class StringExtension
    {
        public static bool EqualsIgnore(this string str, string value)
        {
            return str.Equals(value, StringComparison.OrdinalIgnoreCase);

        }
    }
}
