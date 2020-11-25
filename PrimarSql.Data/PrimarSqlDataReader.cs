using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using PrimarSql.Data.Providers;

namespace PrimarSql.Data
{
    public sealed class PrimarSqlDataReader : DbDataReader
    {
        private readonly IDataProvider _dataProvider;
        
        public override int FieldCount => _dataProvider.GetSchemaTable()?.Rows.Count ?? 0;

        public override bool HasRows => _dataProvider.HasRows;

        public override int RecordsAffected => _dataProvider.RecordsAffected;

        public override bool IsClosed => false;

        public override int Depth => 0;

        internal PrimarSqlDataReader(IDataProvider dataProvider)
        {
            _dataProvider = dataProvider;
        }

        public override object this[int ordinal] => _dataProvider[ordinal];

        public override object this[string name] => _dataProvider[GetOrdinal(name)];

        public override DataTable GetSchemaTable()
        {
            return _dataProvider.GetSchemaTable();
        }
        
        public override string GetName(int ordinal)
        {
            var dataRow = _dataProvider.GetDataRow(ordinal);
            return dataRow[SchemaTableColumn.ColumnName].ToString();
        }

        public override int GetOrdinal(string name)
        {
            var dataRow = _dataProvider.GetDataRow(name);
            return (int)dataRow[SchemaTableColumn.ColumnOrdinal];
        }

        public override string GetDataTypeName(int ordinal)
        {
            var dataRow = _dataProvider.GetDataRow(ordinal);
            return ((Type)dataRow[SchemaTableColumn.DataType])?.Name;
        }

        public override Type GetFieldType(int ordinal)
        {
            var dataRow = _dataProvider.GetDataRow(ordinal);
            return (Type)dataRow[SchemaTableColumn.DataType];
        }

        public override object GetValue(int ordinal)
        {
            return _dataProvider[ordinal];
        }

        public override int GetValues(object[] values)
        {
            for (var i = 0; i < FieldCount; i++)
                values[i] = GetValue(i);

            return values.Length;
        }

        public override byte GetByte(int ordinal)
        {
            throw new NotSupportedException();
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            throw new NotSupportedException();
        }

        public override char GetChar(int ordinal)
        {
            throw new NotSupportedException();
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            throw new NotSupportedException();
        }

        public override DateTime GetDateTime(int ordinal)
        {
            return (DateTime)GetValue(ordinal);
        }

        public override Guid GetGuid(int ordinal)
        {
            throw new NotSupportedException();
        }

        public override short GetInt16(int ordinal)
        {
            return (short)GetValue(ordinal);
        }

        public override int GetInt32(int ordinal)
        {
            return (int)GetValue(ordinal);
        }

        public override long GetInt64(int ordinal)
        {
            return (long)GetValue(ordinal);
        }

        public override float GetFloat(int ordinal)
        {
            return (float)GetValue(ordinal);
        }

        public override double GetDouble(int ordinal)
        {
            return (double)GetValue(ordinal);
        }

        public override decimal GetDecimal(int ordinal)
        {
            return (decimal)GetValue(ordinal);
        }

        public override bool GetBoolean(int ordinal)
        {
            return (bool)GetValue(ordinal);
        }

        public override string GetString(int ordinal)
        {
            return GetValue(ordinal).ToString();
        }

        public override IEnumerator GetEnumerator()
        {
            return new PrimarSqlEnumerator(this);
        }

        public override bool NextResult()
        {
            return false;
        }

        public override bool Read() => _dataProvider.Next();

        public override bool IsDBNull(int ordinal)
        {
            return GetValue(ordinal) == DBNull.Value;
        }

        internal sealed class PrimarSqlEnumerator : IEnumerator
        {
            public object Current => _dataReader._dataProvider.Current;

            private readonly PrimarSqlDataReader _dataReader;

            public PrimarSqlEnumerator(PrimarSqlDataReader dataReader)
            {
                _dataReader = dataReader;
            }

            public bool MoveNext()
            {
                return _dataReader.Read();
            }

            public void Reset()
            {
                throw new NotSupportedException();
            }
        }
    }
}
