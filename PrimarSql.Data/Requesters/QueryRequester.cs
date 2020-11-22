using System.Linq;
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

            return request;
        }

        protected override RequestResponseData GetResponse(QueryRequest request)
        {
            var response = Client.QueryAsync(request).Result;

            return new RequestResponseData
            {
                Items = response.Items,
                ExclusiveStartKey = response.LastEvaluatedKey
            };
        }
    }
}
