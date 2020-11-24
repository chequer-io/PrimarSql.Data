using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Amazon.DynamoDBv2.Model;
using Newtonsoft.Json.Linq;
using PrimarSql.Data.Utilities;

namespace PrimarSql.Data.Processors
{
    internal sealed class StarProcessor : BaseProcessor
    {
        private DataTable _schemaTable;

        public override DataTable GetSchemaTable()
        {
            if (_schemaTable == null)
            {
                _schemaTable = DataProviderUtility.GetNewSchemaTable();
                _schemaTable.Rows.Add("Document", 0, typeof(object), null, false);
            }

            return _schemaTable;
        }

        public override JToken[] Process(Dictionary<string, AttributeValue> row)
        {
            return new JToken[] { row.ToJObject() };
        }
    }
}
