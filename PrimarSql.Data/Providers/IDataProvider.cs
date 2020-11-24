using System.Data;
using Newtonsoft.Json.Linq;

namespace PrimarSql.Data.Providers
{
    internal interface IDataProvider
    {
        object[] Current { get; }
        
        object this[int i] { get; }
        
        bool HasRows { get; }

        int RecordsAffected { get; }

        object GetData(int ordinal);

        DataRow GetDataRow(int ordinal);
        
        DataRow GetDataRow(string name);
        
        DataTable GetSchemaTable();

        bool Next();
    }
}
