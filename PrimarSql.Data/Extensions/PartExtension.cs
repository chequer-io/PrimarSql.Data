using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
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

                        var identifier = identifierPart.Identifier;
                        var needEscape = !Regex.IsMatch(identifier, @"[a-z_$0-9]*?[a-z_$]+?[a-z_$0-9]*", RegexOptions.IgnoreCase);

                        sb.Append(needEscape ? $"'{identifier.Replace("'", "''")}'" : identifier);

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
