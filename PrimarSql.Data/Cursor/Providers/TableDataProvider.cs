using System;
using System.Collections.Generic;
using System.Linq;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Extensions;
using PrimarSql.Data.Models;
using PrimarSql.Data.Sources;

namespace PrimarSql.Data.Cursor.Providers
{
    public class TableDataProvider : IDataProvider
    {
        private DataCell[][] _datas;
        private int _index = 0;

        public QueryContext Context { get; }

        public SelectQueryInfo QueryInfo { get; }

        public bool HasRows { get; }

        public DataCell[] Current { get; private set; }

        public TableDataProvider(QueryContext context, SelectQueryInfo queryInfo)
        {
            Context = context;
            QueryInfo = queryInfo;
        }

        public bool Next()
        {
            if (_datas == null || _datas.Length <= _index)
            {
                _index = 0;
                if (!Fetch())
                    return false;
            }

            Current = _datas[_index++];
            
            return true;
        }

        // TODO: WHERE Expression
        public bool Fetch()
        {
            if (!(QueryInfo.TableSource is AtomTableSource atomTableSource))
                return false;

            var request = new ScanRequest
            {
                TableName = atomTableSource.TableName
            };

            var scanResult = Context.Client.ScanAsync(request).Result;

            _datas = scanResult.Items.Select(dict =>
                dict.Select(c =>
                {
                    var value = c.Value.ToValue();
                    var dataType = value.GetType();

                    return new DataCell
                    {
                        Data = value,
                        DataType = dataType,
                        TypeName = dataType.Name,
                        Name = c.Key,
                    };
                }).ToArray()
            ).ToArray();

            return true;
        }
    }
}
