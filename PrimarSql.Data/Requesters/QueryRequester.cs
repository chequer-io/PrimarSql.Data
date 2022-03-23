using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Models;

namespace PrimarSql.Data.Requesters
{
    internal sealed class QueryRequester : MultiValueRequester<QueryRequest>
    {
        protected override void Initialize()
        {
            if (QueryInfo.Limit == 0)
            {
                HasRows = false;
                return;
            }

            if (QueryInfo.Offset != -1)
                RemainedSkipCount = QueryInfo.Offset;
        }

        protected override QueryRequest GetRequest()
        {
            var request = new QueryRequest
            {
                TableName = TableName,
                ExpressionAttributeNames = ExpressionAttributeNames.ToDictionary(kv => kv.Key, kv => kv.Value),
                ExpressionAttributeValues = ExpressionAttributeValues.ToDictionary(kv => kv.Key, kv => kv.Value),
                ScanIndexForward = !QueryInfo.OrderDescend,
                FilterExpression = string.IsNullOrWhiteSpace(FilterExpression) ? null : FilterExpression.Trim(),
                KeyConditionExpression = HashKey + (SortKey != null ? " AND " + SortKey : string.Empty),
                ExclusiveStartKey = ExclusiveStartKey,
            };

            if (!string.IsNullOrEmpty(IndexName))
                request.IndexName = IndexName;

            return request;
        }

        protected override QueryRequest GetSkipRequest(QueryRequest request)
        {
            request.Limit = RemainedSkipCount;
            return request;
        }

        protected override QueryRequest GetFetchRequest(QueryRequest request)
        {
            if (RemainedCount != -1)
                request.Limit = RemainedCount;

            if (PreventData)
                request.Select = Select.COUNT;

            return request;
        }

        protected override async Task<RequestResponseData> GetResponseAsync(QueryRequest request, CancellationToken cancellationToken = default)
        {
            var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, Command.CancellationTokenSource.Token).Token;
            var response = await Client.QueryAsync(request, cts);

            IEnumerable<Dictionary<string, AttributeValue>> value;

            if (PreventData)
                value = new EmptyEnumerable<Dictionary<string, AttributeValue>>(response.Count);
            else
                value = response.Items;

            return new RequestResponseData
            {
                Items = value,
                ExclusiveStartKey = response.LastEvaluatedKey
            };
        }
    }
}
