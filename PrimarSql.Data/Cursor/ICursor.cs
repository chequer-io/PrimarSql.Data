using System;
using System.Data;
using PrimarSql.Data.Processors;

namespace PrimarSql.Data.Cursor
{
    // TODO: internal
    public interface ICursor : IDisposable
    {
        IProcessor Processor { get; set; }
        
        bool IsClosed { get; }
        
        bool HasRows { get; }

        int RecordsAffected { get; }
        
        int FieldCount { get; }
        
        DataTable GetSchemaTable();

        object this[int i] { get; }
        
        object this[string name] { get; }

        int GetOrdinal(string name);
        
        string Getname(int i);

        string GetDataTypeName(int i);

        Type GetFieldType(int i);
        
        object GetData(int ordinal);

        object[] GetDatas();
        
        bool Read();
    }
}
