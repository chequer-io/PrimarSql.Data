using System.Linq;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Models;

namespace PrimarSql.Data.Requesters
{
    public class ScanRequester : MultiValueRequester<ScanRequest>
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

        protected override ScanRequest GetRequest()
        {
            var request = new ScanRequest()
            {
                TableName = TableName,
                ExpressionAttributeNames = ExpressionAttributeNames.ToDictionary(kv => kv.Key, kv => kv.Value),
                ExpressionAttributeValues = ExpressionAttributeValues.ToDictionary(kv => kv.Key, kv => kv.Value),
                FilterExpression = string.IsNullOrWhiteSpace(FilterExpression) ? null : FilterExpression.Trim(),
                ExclusiveStartKey = ExclusiveStartKey,
            };

            if (!string.IsNullOrEmpty(IndexName))
                request.IndexName = IndexName;

            return request;
        }

        protected override ScanRequest GetSkipRequest(ScanRequest request)
        {
            request.Limit = RemainedSkipCount;
            return request;
        }

        protected override ScanRequest GetFetchRequest(ScanRequest request)
        {
            if (RemainedCount != -1)
                request.Limit = RemainedCount;

            return request;
        }

        protected override RequestResponseData GetResponse(ScanRequest request)
        {
            var response = Client.ScanAsync(request).Result;

            return new RequestResponseData
            {
                Items = response.Items,
                ExclusiveStartKey = response.LastEvaluatedKey
            };
        }
    }
}
