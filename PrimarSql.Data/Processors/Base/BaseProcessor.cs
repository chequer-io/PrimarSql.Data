using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using Amazon.DynamoDBv2.Model;
using Newtonsoft.Json.Linq;
using PrimarSql.Data.Extensions;
using PrimarSql.Data.Models.Columns;

namespace PrimarSql.Data.Processors
{
    internal abstract class BaseProcessor : IProcessor
    {
        public virtual Dictionary<string, AttributeValue> Current { get; set; }

        public abstract IColumn[] Columns { get; }
        
        public abstract DataTable GetSchemaTable();

        public virtual DataRow GetDataRow(string name)
        {
            var schemaTable = GetSchemaTable();

            IEnumerable<DataRow> matchedRows = schemaTable.Rows
                .Cast<DataRow>()
                .Where(n => n[SchemaTableColumn.ColumnName].ToString() == name);

            return matchedRows.FirstOrDefault();
        }
        
        public virtual DataRow GetDataRow(int ordinal)
        {
            var schemaTable = GetSchemaTable();

            IEnumerable<DataRow> matchedRows = schemaTable.Rows
                .Cast<DataRow>()
                .Where(n => (int)n[SchemaTableColumn.ColumnOrdinal] == ordinal);

            return matchedRows.FirstOrDefault();
        }

        public abstract object[] Process();

        public abstract Dictionary<string, AttributeValue> Filter();
    }
}
