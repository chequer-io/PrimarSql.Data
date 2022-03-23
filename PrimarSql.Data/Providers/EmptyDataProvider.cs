using System.Data;
using System.Threading;
using System.Threading.Tasks;
using PrimarSql.Data.Processors;

namespace PrimarSql.Data.Providers
{
    internal sealed class EmptyDataProvider : IDataProvider
    {
        public PrimarSqlCommand Command => null;

        public IProcessor Processor => null;

        public object[] Current => null;

        public object this[int i] => null;

        public bool HasRows => false;

        public int RecordsAffected { get; }

        public EmptyDataProvider(int recordsAffected = 0)
        {
            RecordsAffected = recordsAffected;
        }

        public object GetData(int ordinal)
        {
            return null;
        }

        public DataRow GetDataRow(int ordinal)
        {
            return null;
        }

        public DataRow GetDataRow(string name)
        {
            return null;
        }

        public DataTable GetSchemaTable()
        {
            return null;
        }

        public bool Next()
        {
            return false;
        }

        public Task<bool> NextAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult(false);
        }

        public void Dispose()
        {
        }
    }
}
