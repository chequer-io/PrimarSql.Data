using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using PrimarSql.Data.Models.Columns;
using PrimarSql.Data.Processors;
using PrimarSql.Data.Utilities;

namespace PrimarSql.Data.Providers
{
    internal sealed class ListDataProvider : IDataProvider
    {
        public PrimarSqlCommand Command => null;

        public IProcessor Processor => null;

        private bool _isDisposed;
        private DataTable _schemaTable;
        private List<(string, Type)> _columns = new List<(string, Type)>();
        private List<object[]> _rows = new List<object[]>();
        private int _index;

        public object this[int i] => GetData(i);

        public bool HasRows => !_isDisposed && _rows.Count > 0;

        public bool HasMoreRows => !_isDisposed && _rows.Count > _index;

        public int RecordsAffected => -1;

        public object[] Current { get; private set; }

        public void AddColumn(string name, Type columnType)
        {
            VerifyNotDisposed();
            _columns.Add((name, columnType));
        }

        public void AddRow(params object[] row)
        {
            VerifyNotDisposed();
            _rows.Add(row);
        }

        public void AddRows(params object[][] rows)
        {
            VerifyNotDisposed();
            _rows.AddRange(rows);
        }

        public DataTable GetSchemaTable()
        {
            VerifyNotDisposed();
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
            VerifyNotDisposed();
            var data = Current[ordinal];

            return data switch
            {
                null => DBNull.Value,
                JValue jValue => jValue.Value,
                _ => data
            };
        }

        public DataRow GetDataRow(string name)
        {
            VerifyNotDisposed();
            var schemaTable = GetSchemaTable();

            IEnumerable<DataRow> matchedRows = schemaTable.Rows
                .Cast<DataRow>()
                .Where(n => n[SchemaTableColumn.ColumnName].ToString() == name);

            return matchedRows.FirstOrDefault();
        }

        public DataRow GetDataRow(int ordinal)
        {
            VerifyNotDisposed();
            var schemaTable = GetSchemaTable();

            IEnumerable<DataRow> matchedRows = schemaTable.Rows
                .Cast<DataRow>()
                .Where(n => (int)n[SchemaTableColumn.ColumnOrdinal] == ordinal);

            return matchedRows.FirstOrDefault();
        }

        public bool Next()
        {
            VerifyNotDisposed();

            if (!HasMoreRows)
                return false;

            Current = _rows[_index++];

            return true;
        }

        public Task<bool> NextAsync(CancellationToken cancellationToken = default)
        {
            return Task.FromResult(Next());
        }

        public void Dispose()
        {
            if (_isDisposed)
                return;

            _schemaTable?.Dispose();
            _columns = null;
            _rows = null;
            _isDisposed = true;
        }

        private void VerifyNotDisposed()
        {
            if (_isDisposed)
                throw new ObjectDisposedException("ListDataProvider is already disposed.");
        }
    }
}
