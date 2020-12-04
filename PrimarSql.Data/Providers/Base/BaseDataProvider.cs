using System;
using System.Data;
using System.Linq;
using PrimarSql.Data.Models.Columns;
using PrimarSql.Data.Planners;
using PrimarSql.Data.Processors;

namespace PrimarSql.Data.Providers
{
    internal abstract class BaseDataProvider : IDataProvider
    {
        public IProcessor Processor { get; set; }
        
        public abstract object[] Current { get; }

        public object this[int i] => GetData(i);

        public abstract bool HasRows { get; }

        public abstract int RecordsAffected { get; }

        public abstract object GetData(int ordinal);

        public DataRow GetDataRow(int ordinal)
        {
            return Processor.GetDataRow(ordinal);
        }

        public DataRow GetDataRow(string name)
        {
            return Processor.GetDataRow(name);
        }

        public DataTable GetSchemaTable()
        {
            return Processor.GetSchemaTable();
        }

        public abstract bool Next();
        
        protected static IProcessor GetProcessor(SelectQueryInfo queryInfo)
        {
            if (queryInfo.Columns.FirstOrDefault() is StarColumn)
            {
                return new StarProcessor();
            }
            else if (queryInfo.Columns.All(c => c is PropertyColumn))
            {
                return new ColumnProcessor(queryInfo.Columns.Select(c => c as PropertyColumn));
            }
            else if (queryInfo.Columns.FirstOrDefault() is CountFunctionColumn)
            {
                return new CountFunctionProcessor();
            }
            else
            {
                throw new NotSupportedException("Not supported column type");
            }
        }
    }
}
