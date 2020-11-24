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
    internal sealed class ArrayDataColumn
    {
        public string Name { get; set; }

        public Type Type { get; set; }
    }

    internal sealed class ArrayDataProvider : IDataProvider
    {
        private DataTable _schemaTable;
        private readonly ArrayDataColumn[] _columns;
        private readonly object[][] _datas;
        private int _index;

        public object this[int i] => GetData(i);

        public bool HasRows => _datas.Length > 0;

        public bool HasMoreRows => _datas.Length > _index + 1;

        public int RecordsAffected => -1;

        public object[] Current { get; private set; }

        public ArrayDataProvider(object[][] datas, ArrayDataColumn[] columns)
        {
            _columns = columns;
            _datas = datas;
        }

        public DataTable GetSchemaTable()
        {
            if (_schemaTable == null)
            {
                _schemaTable = DataProviderUtility.GetNewSchemaTable();

                int i = 0;

                foreach (var column in _columns)
                {
                    _schemaTable.Rows.Add(column.Name, i++, column.Type, new IPart[] { new IdentifierPart(column.Name) }, false);
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

            Current = _datas[_index++];

            return true;
        }
    }
}
