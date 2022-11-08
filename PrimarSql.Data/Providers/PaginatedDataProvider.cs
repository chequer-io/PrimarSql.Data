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
    internal abstract class PaginatedDataProvider : IDataProvider
    {
        public PrimarSqlCommand Command => null;

        public IProcessor Processor => null;

        public object this[int i] => GetData(i);

        public bool HasRows => true;

        public int RecordsAffected => -1;

        public object[] Current => _rowsEnumerator.Current;

        private bool _isDisposed;
        private DataTable _schemaTable;
        private readonly (string, Type)[] _columns;
        private IEnumerator<object[]> _rowsEnumerator;
        private bool _isClosed;

        protected PaginatedDataProvider((string, Type)[] columns)
        {
            _columns = columns;
        }

        public object GetData(int ordinal)
        {
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

        protected abstract IEnumerator<object[]> Fetch();

        public bool Next()
        {
            if (_isClosed)
                return false;

            if (_rowsEnumerator is null || !_rowsEnumerator.MoveNext())
            {
                _rowsEnumerator = Fetch();

                if (_rowsEnumerator is null || !_rowsEnumerator.MoveNext())
                {
                    _isClosed = true;
                    return false;
                }
            }

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
            _isDisposed = true;
        }
    }
}
