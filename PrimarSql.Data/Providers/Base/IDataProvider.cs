using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using PrimarSql.Data.Processors;

namespace PrimarSql.Data.Providers
{
    internal interface IDataProvider : IDisposable
    {
        PrimarSqlCommand Command { get; }
        
        IProcessor Processor { get; }

        object[] Current { get; }

        object this[int i] { get; }

        bool HasRows { get; }

        int RecordsAffected { get; }

        object GetData(int ordinal);

        DataRow GetDataRow(int ordinal);

        DataRow GetDataRow(string name);

        DataTable GetSchemaTable();

        bool Next();

        Task<bool> NextAsync(CancellationToken cancellationToken = default);
    }
}
