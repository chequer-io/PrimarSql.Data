using System;
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
        private bool _isFirst = true;

        public ShowTableProvider(AmazonDynamoDBClient client) : base(new[] { ("name", typeof(string)) })
        {
            _client = client;
        }

        protected override IEnumerator<object[]> Fetch()
        {
            var result = _client.ListTablesAsync(new ListTablesRequest
            {
                Limit = 100,
                ExclusiveStartTableName = _lastTableName
            }).Result;

            if (result.TableNames.Count == 0)
                return null;

            _lastTableName = result.LastEvaluatedTableName;
            var enumerator = new InternalEnumerator(result.TableNames, _isFirst);
            _isFirst = false;

            return enumerator;
        }

        private class InternalEnumerator : IEnumerator<object[]>
        {
            private readonly List<string> _tableNames;
            private int _index;
            private bool _isInitialized;

            public InternalEnumerator(List<string> tableNames, bool skipFirst)
            {
                _tableNames = tableNames;

                _index = skipFirst ? 0 : -1;
            }

            public bool MoveNext()
            {
                if (_index >= _tableNames.Count - 1)
                    return false;

                _index++;
                _isInitialized = true;
                return true;
            }

            public void Reset()
            {
                _index = 0;
            }

            public object[] Current => _isInitialized ? new object[] { _tableNames[_index] } : null;

            object IEnumerator.Current => Current;

            public void Dispose()
            {
            }
        }
    }
}
