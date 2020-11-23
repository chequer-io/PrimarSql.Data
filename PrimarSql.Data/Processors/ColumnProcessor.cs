using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using Amazon.DynamoDBv2.Model;
using Newtonsoft.Json.Linq;
using PrimarSql.Data.Models.Columns;
using PrimarSql.Data.Utilities;

namespace PrimarSql.Data.Processors
{
    internal sealed class ColumnProcessor : BaseProcessor
    {
        private readonly DataTable _schemaTable;

        public ColumnProcessor(IEnumerable<PropertyColumn> columns)
        {
            _schemaTable = new DataTable();
            _schemaTable.Columns.Add(SchemaTableColumn.ColumnName, typeof(string));
            _schemaTable.Columns.Add(SchemaTableColumn.ColumnOrdinal, typeof(int));
            _schemaTable.Columns.Add(SchemaTableColumn.DataType, typeof(Type));
            _schemaTable.Columns.Add("Path", typeof(IPart[]));
            _schemaTable.Columns.Add(SchemaTableOptionalColumn.IsReadOnly, typeof(bool));

            foreach (var column in columns)
            {
                string name = string.IsNullOrEmpty(column.Alias) ? ConverToName(column.Name) : column.Alias;

                _schemaTable.Rows.Add(name, 0, typeof(object), column.Name, false);
            }
        }

        public override DataTable GetSchemaTable()
        {
            return _schemaTable;
        }

        public override JToken[] Process(Dictionary<string, AttributeValue> row)
        {
            var jObject = row.ToJObject();

            return _schemaTable.Rows
                .Cast<DataRow>()
                .Select(dataRow => SelectToken(jObject, (IPart[])dataRow["path"])).ToArray();
        }

        private string ConverToName(IEnumerable<IPart> parts)
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

        private JToken SelectToken(JToken token, IEnumerable<IPart> parts)
        {
            var currentToken = token;

            foreach (var part in parts)
            {
                switch (part)
                {
                    case IdentifierPart identifierPart when currentToken is JObject jObject:
                        currentToken = jObject[identifierPart.Identifier];
                        break;

                    case IndexPart indexPart when currentToken is JArray jArray:
                        currentToken = jArray[indexPart.Index];
                        break;

                    default:
                        return null;
                }

                if (currentToken == null)
                    return null;
            }

            return currentToken;
        }
    }
}
