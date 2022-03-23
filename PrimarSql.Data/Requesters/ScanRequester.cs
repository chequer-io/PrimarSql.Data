using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Models;

namespace PrimarSql.Data.Requesters
{
    internal sealed class ScanRequester : MultiValueRequester<ScanRequest>
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

            if (PreventData)
                request.Select = Select.COUNT;

            return request;
        }

        protected override async Task<RequestResponseData> GetResponseAsync(ScanRequest request, CancellationToken cancellationToken = default)
        {
            var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, Command.CancellationTokenSource.Token).Token;
            var response = await Client.ScanAsync(request, cts);

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
