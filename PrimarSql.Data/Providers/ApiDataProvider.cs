using System;
using System.Data;
using System.Linq;
using Amazon.DynamoDBv2.Model;
using Newtonsoft.Json.Linq;
using PrimarSql.Data.Expressions.Generators;
using PrimarSql.Data.Models;
using PrimarSql.Data.Models.Columns;
using PrimarSql.Data.Planners;
using PrimarSql.Data.Processors;
using PrimarSql.Data.Requesters;
using PrimarSql.Data.Sources;

namespace PrimarSql.Data.Providers
{
    internal sealed class ApiDataProvider : IDataProvider
    {
        private readonly TableDescription _tableDescription;
        private IRequester _requester;
        private IProcessor _processor;

        public QueryContext Context { get; }

        public SelectQueryInfo QueryInfo { get; }

        public object this[int i] => GetData(i);

        public bool HasRows => _requester.HasRows;

        public int RecordsAffected => -1;

        public JToken[] Current { get; private set; }

        public AtomTableSource AtomTableSource => QueryInfo.TableSource as AtomTableSource;
        
        public string TableName => AtomTableSource?.TableName;

        public string IndexName => AtomTableSource?.IndexName;

        public ApiDataProvider(QueryContext context, SelectQueryInfo queryInfo)
        {
            Context = context;
            QueryInfo = queryInfo;

            _tableDescription = context.GetTableDescription(TableName);

            SetProcessor();
            SetRequester();
        }

        public DataTable GetSchemaTable()
        {
            return _processor.GetSchemaTable();
        }

        public object GetData(int ordinal)
        {
            var data = Current[ordinal];

            return data switch
            {
                null => DBNull.Value,
                JValue jValue => jValue.Value,
                _ => data.ToString()
            };
        }

        public DataRow GetDataRow(string name)
        {
            return _processor.GetDataRow(name);
        }

        public DataRow GetDataRow(int ordinal)
        {
            return _processor.GetDataRow(ordinal);
        }

        public bool Next()
        {
            var flag = _requester.Next();

            Current = flag ? _processor.Process(_requester.Current) : null;

            return flag;
        }

        #region Intalize Processor/Requester
        private void SetProcessor()
        {
            if (QueryInfo.Columns.FirstOrDefault() is StarColumn)
            {
                _processor = new StarProcessor();
            }
            else if (QueryInfo.Columns.All(c => c is PropertyColumn))
            {
                _processor = new ColumnProcessor(QueryInfo.Columns.Select(c => c as PropertyColumn));    
            }
            else
            {
                throw new NotSupportedException("Not supported column type");
            }
        }

        private void SetRequester()
        {
            var generator = new ExpressionGenerator(_tableDescription, IndexName, QueryInfo.WhereExpression);
            var generateResult = generator.Analyze();

            var sortKeyExists = !string.IsNullOrEmpty(generator.SortKeyName);

            // Hash key only in this index
            if (!sortKeyExists)
            {
                // If HashKey not found in where expr
                if (generateResult.HashKey == null)
                    _requester = new ScanRequester();
                else
                {
                    if (string.IsNullOrEmpty(IndexName) && string.IsNullOrWhiteSpace(generateResult.FilterExpression))
                        _requester = new GetItemRequester();
                    else
                        _requester = new QueryRequester();
                }
            }
            // HashKey and SortKey
            else
            {
                // If HashKey or SortKey not found in where expr
                if (generateResult.HashKey == null &&
                    generateResult.SortKey == null)
                    _requester = new ScanRequester();
                else
                {
                    if (string.IsNullOrEmpty(IndexName) &&
                        string.IsNullOrWhiteSpace(generateResult.FilterExpression) &&
                        generateResult.SortKey?.Operator == "=")
                        _requester = new GetItemRequester();
                    else
                    {
                        _requester = new QueryRequester();
                    }
                }
            }

            _requester.SetParameters(
                Context.Client,
                QueryInfo,
                generateResult.ExpressionAttributeNames,
                generateResult.ExpressionAttributeValues,
                generateResult.HashKey,
                generateResult.SortKey,
                TableName,
                IndexName,
                generateResult.FilterExpression
            );
        }
        #endregion
    }
}
