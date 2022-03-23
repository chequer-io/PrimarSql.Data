using System;
using System.Threading;
using System.Threading.Tasks;
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
        public QueryContext Context { get; }

        public SelectQueryInfo QueryInfo { get; }

        public override bool HasRows => _requester.HasRows;

        public override int RecordsAffected => -1;

        public override object[] Current => _current;

        public AtomTableSource AtomTableSource => QueryInfo.TableSource as AtomTableSource;

        public string TableName => AtomTableSource?.TableName;

        public string IndexName => AtomTableSource?.IndexName;

        private bool _isDisposed;
        private TableDescription _tableDescription;
        private bool _isInitialized;
        private IRequester _requester;
        private object[] _current;

        public ApiDataProvider(QueryContext context, SelectQueryInfo queryInfo)
        {
            Context = context;
            QueryInfo = queryInfo;
        }

        public async Task InitializeAsync(CancellationToken cancellationToken = default)
        {
            VerifyNotDisposed();

            if (_isInitialized)
                return;

            _tableDescription = await Context.GetTableDescriptionAsync(TableName, cancellationToken);

            Processor = GetProcessor(QueryInfo);
            SetRequester();

            _isInitialized = true;
        }

        public override object GetData(int ordinal)
        {
            VerifyNotDisposed();
            var data = Current[ordinal];

            switch (data)
            {
                case null:
                    return DBNull.Value;

                case JValue jValue:
                    return jValue.Value;

                case string _:
                case short _:
                case ushort _:
                case int _:
                case uint _:
                case long _:
                case ulong _:
                case decimal _:
                case double _:
                case bool _:
                case char _:
                    return data;

                default:
                    return data.ToString();
            }
        }

        public override bool Next()
        {
            return !Context.Command.IsCanceled && NextAsync().Result;
        }

        public override async Task<bool> NextAsync(CancellationToken cancellationToken = default)
        {
            VerifyNotDisposed();

            if (Context.Command.IsCanceled)
                return false;

            if (Processor is CountFunctionProcessor processor)
            {
                if (processor.Read)
                    return false;

                processor.Read = true;
                _current = new object[] { _requester.RequestCount() };
                return true;
            }

            var flag = await _requester.NextAsync(cancellationToken);
            Processor.Current = _requester.Current;

            if (Context.DocumentFilters != null)
            {
                foreach (var documentFilter in Context.DocumentFilters)
                    documentFilter.Filter(TableName, Processor.Current);
            }

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

            _requester.Command = Command;

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

        public override void Dispose()
        {
            if (_isDisposed)
                return;

            _requester = null;
            _current = null;
            _tableDescription = null;

            _isDisposed = true;
        }

        private void VerifyNotDisposed()
        {
            if (_isDisposed)
                throw new ObjectDisposedException("ListDataProvider is already disposed.");
        }
    }
}
