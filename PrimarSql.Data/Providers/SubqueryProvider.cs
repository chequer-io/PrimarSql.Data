using System;
using Newtonsoft.Json.Linq;
using PrimarSql.Data.Models;
using PrimarSql.Data.Planners;
using PrimarSql.Data.Processors;
using PrimarSql.Data.Sources;

namespace PrimarSql.Data.Providers
{
    internal sealed class SubqueryProvider : BaseDataProvider
    {
        private readonly IDataProvider _nestedDataProvider;
        private int _count = 0;
        private object[] _current;
        private bool _hasMoreRows = true;

        public override object[] Current => _current;

        public QueryContext Context { get; }

        public SelectQueryInfo QueryInfo { get; }

        public override bool HasRows => _nestedDataProvider.HasRows;

        public bool HasMoreRows => _hasMoreRows && (QueryInfo.Limit == -1 || _count < QueryInfo.Limit);
        
        public override int RecordsAffected => -1;
        
        public SubqueryProvider(QueryContext context, SelectQueryInfo queryInfo)
        {
            Context = context;
            QueryInfo = queryInfo;
            
            if (!(queryInfo.TableSource is SubquerySource subquerySource))
                throw new InvalidOperationException($"Cannot handle {queryInfo.GetType().Name} in SubqueryProvider.");

            _nestedDataProvider = DataProviderFactory.Create(context, subquerySource.SubqueryInfo);
            Processor = new SubqueryProcessor(GetProcessor(QueryInfo), _nestedDataProvider.Processor);
        }

        public override object GetData(int ordinal)
        {
            var data = Current[ordinal];

            return data switch
            {
                null => DBNull.Value,
                JValue jValue => jValue.Value,
                _ => data.ToString()
            };
        }

        public override bool Next()
        {
            if (!HasMoreRows)
                return false;
            
            var flag = _nestedDataProvider.Next();
            
            if (!flag)
                _hasMoreRows = false;

            _count++;
            _current = flag ? Processor.Process() : null;
            
            return flag;
        }
    }
}
