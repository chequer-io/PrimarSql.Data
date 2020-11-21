using System.Data;
using PrimarSql.Data.Models;

namespace PrimarSql.Data.Cursor.Providers
{
    public interface IDataProvider
    {
        bool HasRows { get; }

        DataCell[] Current { get; }

        DataTable GetSchemaTable();

        bool Next();
    }
}
