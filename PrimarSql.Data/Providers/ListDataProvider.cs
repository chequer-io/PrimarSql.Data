using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using Newtonsoft.Json.Linq;
using PrimarSql.Data.Models.Columns;
using PrimarSql.Data.Utilities;

namespace PrimarSql.Data.Providers
{
    internal sealed class ListDataProvider : IDataProvider
    {
        private DataTable _schemaTable;
        private readonly List<(string, Type)> _columns = new List<(string, Type)>();
        private readonly List<object[]> _rows = new List<object[]>();
        private int _index;

        public object this[int i] => GetData(i);

        public bool HasRows => _rows.Count > 0;

        public bool HasMoreRows => _rows.Count > _index;

        public int RecordsAffected => -1;

        public object[] Current { get; private set; }

        public void AddColumn(string name, Type columnType)
        {
            _columns.Add((name, columnType));
        }

        public void AddRow(params object[] row)
        {
            _rows.Add(row);
        }

        public void AddRows(params object[][] rows)
        {
            _rows.AddRange(rows);
        }

        public DataTable GetSchemaTable()
        {
            if (_schemaTable == null)
            {
                _schemaTable = DataProviderUtility.GetNewSchemaTable();

                int i = 0;

                foreach (var (name, type) in _columns)
                {
                    _schemaTable.Rows.Add(name, i++, type, new IPart[] { new IdentifierPart(name) }, false);
                }
            }

            return _schemaTable;
        }

        public object GetData(int ordinal)
        {
            var data = Current[ordinal];

            return data switch
            {
                null => DBNull.Value,
                JValue jValue => jValue.Value,
                _ => data.ToString()
            };
        }

        public DataRow GetDataRow(string name)
        {
            var schemaTable = GetSchemaTable();

            IEnumerable<DataRow> matchedRows = schemaTable.Rows
                .Cast<DataRow>()
                .Where(n => n[SchemaTableColumn.ColumnName].ToString() == name);

            return matchedRows.FirstOrDefault();
        }

        public DataRow GetDataRow(int ordinal)
        {
            var schemaTable = GetSchemaTable();

            IEnumerable<DataRow> matchedRows = schemaTable.Rows
                .Cast<DataRow>()
                .Where(n => (int)n[SchemaTableColumn.ColumnOrdinal] == ordinal);

            return matchedRows.FirstOrDefault();
        }

        public bool Next()
        {
            if (!HasMoreRows)
                return false;

            Current = _rows[_index++];

            return true;
        }
    }
}
