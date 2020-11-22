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
    public class ColumnProcessor : BaseProcessor
    {
        private readonly DataTable _schemaTable;

        public ColumnProcessor(IEnumerable<PropertyColumn> columns)
        {
            _schemaTable = new DataTable();
            _schemaTable.Columns.Add(SchemaTableColumn.ColumnName, typeof(string));
            _schemaTable.Columns.Add(SchemaTableColumn.ColumnOrdinal, typeof(int));
            _schemaTable.Columns.Add(SchemaTableColumn.DataType, typeof(Type));
            _schemaTable.Columns.Add("Path", typeof(object[]));
            _schemaTable.Columns.Add(SchemaTableOptionalColumn.IsReadOnly, typeof(bool));

            foreach (var column in columns)
            {
                string name = string.IsNullOrEmpty(column.Alias) ?
                    string.Join(".", column.Name.Select(n => $"`{n}`")) :
                    column.Alias;

                _schemaTable.Rows.Add(name, 0, typeof(object), column.Name, false);
            }
        }

        public override DataTable GetSchemaTable()
        {
            return _schemaTable;
        }

        private string BuildPath(IEnumerable<object> obj)
        {
            return string.Join("", obj.Select(o =>
            {
                switch (o)
                {
                    case string s:
                        return $"['{s}']";

                    case int i:
                        return $"[{i}]";
                }

                return string.Empty;
            }));
        }

        public override JToken[] Process(Dictionary<string, AttributeValue> row)
        {
            var jObject = row.ToJObject();

            return _schemaTable.Rows.Cast<DataRow>().Select(dataRow =>
            {
                var name = "$" + BuildPath((object[])dataRow["path"]);

                return jObject.SelectToken(name);
            }).ToArray();
        }
    }
}
