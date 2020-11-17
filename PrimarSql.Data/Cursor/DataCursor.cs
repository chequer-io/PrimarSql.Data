using System;
using System.Data;
using System.Linq;
using PrimarSql.Data.Extensions;
using PrimarSql.Data.Models;

namespace PrimarSql.Data.Cursor
{
    public abstract class DataCursor : ICursor
    {
        protected DataCell[] CurrentRow { get; set; }

        public abstract bool IsClosed { get; }

        public abstract bool HasRows { get; }

        public abstract int RecordsAffected { get; }

        public virtual int FieldCount => CurrentRow?.Length ?? 0;

        public abstract DataTable GetSchemaTable();

        public virtual object this[int i] => GetData(i);

        public virtual object this[string name] => GetData(GetOrdinal(name));

        public virtual int GetOrdinal(string name)
        {
            return CurrentRow.IndexOf(r => r.Name == name);
        }
        
        public virtual string Getname(int i)
        {
            return CurrentRow[i].Name;
        }

        public virtual string GetDataTypeName(int i)
        {
            return CurrentRow[i].TypeName;
        }

        public virtual Type GetFieldType(int i)
        {
            return CurrentRow[i].DataType;
        }

        public virtual object GetData(int ordinal)
        {
            return CurrentRow[ordinal].Data;
        }

        public virtual object[] GetDatas() => CurrentRow.Select(cell => cell.Data).ToArray();
        
        public abstract bool Read();

        public virtual void Dispose()
        {
        }
    }
}
