using System;
using System.Data;
using Amazon.DynamoDBv2.Model;
using Newtonsoft.Json.Linq;
using PrimarSql.Data.Expressions.Generators;
using PrimarSql.Data.Models;
using PrimarSql.Data.Planners;
using PrimarSql.Data.Processors;
using PrimarSql.Data.Requesters;
using PrimarSql.Data.Sources;

namespace PrimarSql.Data.Providers
{
    internal sealed class ApiDataProvider : BaseDataProvider
    {
        private readonly TableDescription _tableDescription;
        private IRequester _requester;
        private object[] _current;

        public QueryContext Context { get; }

        public SelectQueryInfo QueryInfo { get; }

        public override bool HasRows => _requester.HasRows;

        public override int RecordsAffected => -1;

        public override object[] Current => _current;

        public AtomTableSource AtomTableSource => QueryInfo.TableSource as AtomTableSource;

        public string TableName => AtomTableSource?.TableName;

        public string IndexName => AtomTableSource?.IndexName;

        public ApiDataProvider(QueryContext context, SelectQueryInfo queryInfo)
        {
            Context = context;
            QueryInfo = queryInfo;

            _tableDescription = context.GetTableDescription(TableName);

            Processor = GetProcessor(QueryInfo);
            SetRequester();
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
            if (Processor is CountFunctionProcessor processor)
            {
                if (processor.Read)
                    return false;

                processor.Read = true;
                _current = new object[] { _requester.RequestCount()};
                return true;
            }

            var flag = _requester.Next();

            Processor.Current = _requester.Current;
            _current = flag ? Processor.Process() : null;

            return flag;
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
                generator.HashKeyName,
                generator.SortKeyName,
                TableName,
                IndexName,
                generateResult.FilterExpression
            );
        }
    }
}
