using System.Data.Common;
using System.Linq;
using PrimarSql.Data.Providers;

namespace PrimarSql.Data.Planners.Show
{
    internal sealed class ShowTablePlanner : QueryPlanner<EmptyQueryInfo>
    {
        public ShowTablePlanner()
        {
            QueryInfo = new EmptyQueryInfo();
        }
        
        public override DbDataReader Execute()
        {
            var response = Context.Client.ListTablesAsync().Result;
            object[][] data = response.TableNames.Select(tableName => new[] { (object)tableName }).ToArray();
            var columns = new []
            {
                new ArrayDataColumn
                {
                    Name = "name",
                    Type = typeof(string)
                }
            };

            var provider = new ArrayDataProvider(data, columns);
            return new PrimarSqlDataReader(provider);
        }
    }
}
