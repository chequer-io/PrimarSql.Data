using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Extensions;
using PrimarSql.Data.Models;
using PrimarSql.Data.Sources;

namespace PrimarSql.Data.Cursor
{
    // TODO: Expression
    public class TableCursor : DataCursor
    {
        private Dictionary<string, AttributeValue>[] _datas { get; set; }

        private bool _hasRows = true;

        public QueryContext Context { get; }

        public SelectQueryInfo QueryInfo { get; }

        public override bool IsClosed => false;

        public override bool HasRows => _hasRows;

        public override int RecordsAffected => -1;

        public TableCursor(QueryContext context, SelectQueryInfo queryInfo)
        {
            Context = context;
            QueryInfo = queryInfo;
        }

        public override DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
        }

        public override bool Read()
        {
            Fetch();

            CurrentRow = _datas.Select(data =>
            {
                return data.Select(c =>
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
                }).ToArray();
            }).First();

            return false;
        }

        // TODO: WHERE Expression
        private void Fetch()
        {
            if (QueryInfo.TableSource is AtomTableSource atomTableSource)
            {
                var request = new ScanRequest
                {
                    TableName = atomTableSource.TableName
                };

                var scanResult = Context.Client.ScanAsync(request).Result;
                _datas = scanResult.Items.ToArray();
            }
        }
    }
}
