using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace PrimarSql.Data.Providers
{
    internal sealed class ShowTableProvider : PaginatedDataProvider
    {
        private readonly AmazonDynamoDBClient _client;
        private string _lastEvaluatedTableName;
        private bool _isClosed;

        public ShowTableProvider(AmazonDynamoDBClient client) : base(new[] { ("name", typeof(string)) })
        {
            _client = client;
        }

        protected override IEnumerator<object[]> Fetch()
        {
            return FetchAsync(default).Result;
        }

        protected override async Task<IEnumerator<object[]>> FetchAsync(CancellationToken cancellationToken)
        {
            if (_isClosed)
                return null;

            var response = await _client.ListTablesAsync(new ListTablesRequest
            {
                Limit = 100,
                ExclusiveStartTableName = _lastEvaluatedTableName
            }, cancellationToken);

            if (response.LastEvaluatedTableName is null)
                _isClosed = true;

            _lastEvaluatedTableName = response.LastEvaluatedTableName;

            return new InternalEnumerator(response.TableNames);
        }

        private class InternalEnumerator : IEnumerator<object[]>
        {
            private readonly List<string> _tableNames;
            private int _index = -1;
            private bool _isClosed;

            public InternalEnumerator(List<string> tableNames)
            {
                _tableNames = tableNames;
            }

            public bool MoveNext()
            {
                if (_isClosed)
                    return false;

                _index++;

                if (_index >= _tableNames.Count - 1)
                    _isClosed = true;

                return true;
            }

            public void Reset()
            {
                _index = 0;
            }

            public object[] Current => _index != -1 ? new object[] { _tableNames[_index] } : null;

            object IEnumerator.Current => Current;

            public void Dispose()
            {
            }
        }
    }
}
