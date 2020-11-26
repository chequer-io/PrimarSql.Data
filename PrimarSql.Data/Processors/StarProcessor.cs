using System;
using System.Collections.Generic;
using System.Data;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Models.Columns;
using PrimarSql.Data.Utilities;

namespace PrimarSql.Data.Processors
{
    internal sealed class StarProcessor : BaseProcessor
    {
        private DataTable _schemaTable;

        public override IColumn[] Columns { get; } = { new PropertyColumn("Document") };

        public override DataTable GetSchemaTable()
        {
            if (_schemaTable == null)
            {
                _schemaTable = DataProviderUtility.GetNewSchemaTable();
                _schemaTable.Rows.Add("Document", 0, typeof(object), null, false);
            }

            return _schemaTable;
        }

        public override object[] Process()
        {
            return new object[] { Current.ToJObject() };
        }

        public override Dictionary<string, AttributeValue> Filter()
        {
            return Current;
        }
    }
}
