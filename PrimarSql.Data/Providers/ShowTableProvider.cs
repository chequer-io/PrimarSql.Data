using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Extensions;

namespace PrimarSql.Data.Providers
{
    internal sealed class ShowTableProvider : PaginatedDataProvider
    {
        private readonly IAmazonDynamoDB _client;
        private string _lastEvaluatedTableName;
        private bool _isClosed;

        public ShowTableProvider(IAmazonDynamoDB client) : base(new[] { ("name", typeof(string)) })
        {
            _client = client;
        }

        protected override IEnumerator<object[]> Fetch()
        {
            return FetchAsync(default).GetResultSynchronously();
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

            return response.TableNames
                .Select(x => new object[] { x })
                .GetEnumerator();
        }
    }
}
