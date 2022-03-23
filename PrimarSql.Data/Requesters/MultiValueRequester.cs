using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Extensions;
using PrimarSql.Data.Models;

namespace PrimarSql.Data.Requesters
{
    internal abstract class MultiValueRequester<TRequest> : BaseRequester where TRequest : AmazonDynamoDBRequest
    {
        private int _index;

        protected int RemainedSkipCount { get; set; }

        protected int ReadCount { get; set; }

        protected int RemainedCount => QueryInfo.Limit == -1 ? -1 : QueryInfo.Limit - ReadCount;

        protected List<Dictionary<string, AttributeValue>> Items { get; set; }

        protected Dictionary<string, AttributeValue> ExclusiveStartKey { get; set; }

        protected abstract TRequest GetRequest();

        protected abstract TRequest GetSkipRequest(TRequest request);

        protected abstract TRequest GetFetchRequest(TRequest request);

        protected abstract Task<RequestResponseData> GetResponseAsync(TRequest request, CancellationToken cancellationToken = default);

        public override bool Next()
        {
            return NextAsync().Result;
        }

        public override async Task<bool> NextAsync(CancellationToken cancellationToken = default)
        {
            if (RemainedCount == 0 || RemainedSkipCount != 0 && !await SkipOffsetAsync(cancellationToken))
                return false;

            while (Items == null || Items.Count <= _index)
            {
                if (!HasMoreRows)
                    return false;

                await FetchAsync(cancellationToken);
                _index = 0;
            }

            ReadCount++;
            Current = Items[_index++];

            return true;
        }

        protected virtual async Task<bool> FetchAsync(CancellationToken cancellationToken = default)
        {
            if (!HasRows || !HasMoreRows)
                return false;

            if (ReadCount == 0 && QueryInfo.HasStartKey)
            {
                ExclusiveStartKey = new Dictionary<string, AttributeValue>
                {
                    [HashKeyName] = QueryInfo.StartHashKey.Value.ToAttributeValue()
                };

                if (QueryInfo.StartSortKey != null)
                {
                    ExclusiveStartKey[SortKeyName] = QueryInfo.StartSortKey.Value.ToAttributeValue();
                }
            }

            var request = GetFetchRequest(GetRequest());
            var response = await GetResponseAsync(request, cancellationToken);

            Items = response.Items.ToList();
            ExclusiveStartKey = response.ExclusiveStartKey;

            if (response.ExclusiveStartKey.Count == 0)
                HasMoreRows = false;

            return Items.Count != 0;
        }

        protected virtual async Task<bool> SkipOffsetAsync(CancellationToken cancellationToken = default)
        {
            while (RemainedSkipCount > 0)
            {
                var request = GetSkipRequest(GetRequest());
                var response = await GetResponseAsync(request, cancellationToken);

                ExclusiveStartKey = response.ExclusiveStartKey;

                var count = response.Items.Count();

                if (ExclusiveStartKey.Count == 0 || count == 0)
                    return false;

                RemainedSkipCount -= count;
            }

            return true;
        }

        public override long RequestCount()
        {
            int count = 0;
            PreventData = true;

            while (Next())
            {
                count++;
            }

            return count;
        }
    }
}
