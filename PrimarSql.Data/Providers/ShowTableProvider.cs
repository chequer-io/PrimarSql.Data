using System.Collections;
using System.Collections.Generic;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace PrimarSql.Data.Providers
{
    internal class ShowTableProvider : PaginatedDataProvider
    {
        private readonly AmazonDynamoDBClient _client;
        private string _lastTableName;
        private bool _isClosed;

        public ShowTableProvider(AmazonDynamoDBClient client) : base(new[] { ("name", typeof(string)) })
        {
            _client = client;
        }

        protected override IEnumerator<object[]> Fetch()
        {
            if (_isClosed)
                return null;

            var result = _client.ListTablesAsync(new ListTablesRequest
            {
                Limit = 100,
                ExclusiveStartTableName = _lastTableName
            }).Result;

            if (result.TableNames.Count == 0)
                return null;

            _lastTableName = result.LastEvaluatedTableName;

            if (_lastTableName is null)
                _isClosed = true;

            return new InternalEnumerator(result.TableNames);
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
