using System.Data;
using Newtonsoft.Json.Linq;

namespace PrimarSql.Data.Providers
{
    public interface IDataProvider
    {
        JToken[] Current { get; }
        
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
