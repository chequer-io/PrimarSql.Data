using System.Collections.Generic;
using System.Text;
using PrimarSql.Data.Models.Columns;

namespace PrimarSql.Data.Extensions
{
    internal static class PartExtension
    {
        public static string ToName(this IEnumerable<IPart> parts)
        {
            var sb = new StringBuilder();

            foreach (var part in parts)
            {
                switch (part)
                {
                    case IdentifierPart identifierPart:
                    {
                        if (sb.Length != 0)
                            sb.Append(".");

                        sb.Append($"'{identifierPart.Identifier.Replace("'", "''")}'");
                        break;
                    }

                    case IndexPart indexPart:
                    {
                        sb.Append($"[{indexPart.Index}]");
                        break;
                    }
                }
            }

            return sb.ToString();
        }
    }
}
