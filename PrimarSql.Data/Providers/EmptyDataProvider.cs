using System.Data;
using Newtonsoft.Json.Linq;
using PrimarSql.Data.Processors;

namespace PrimarSql.Data.Providers
{
    internal sealed class EmptyDataProvider : IDataProvider
    {
        public IProcessor Processor { get; } = null;

        public object[] Current => null;

        public object this[int i] => null;

        public bool HasRows => false;

        public int RecordsAffected => 0;

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
    }
}
