using PrimarSql.Data.Models;

namespace PrimarSql.Data.Processors
{
    public interface IProcessor
    {
        DataCell Process(int ordinal);
    }
}
