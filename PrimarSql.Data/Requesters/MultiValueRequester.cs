using System.Collections.Generic;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Models;

namespace PrimarSql.Data.Requesters
{
    internal abstract class MultiValueRequester<TRequest> : BaseRequester where TRequest : AmazonDynamoDBRequest
    {
        private int _index = 0;

        protected int RemainedSkipCount { get; set; }

        protected int ReadCount { get; set; }

        protected int RemainedCount => QueryInfo.Limit == -1 ? -1 : QueryInfo.Limit - ReadCount;
        
        protected List<Dictionary<string, AttributeValue>> Items { get; set; }

        protected Dictionary<string, AttributeValue> ExclusiveStartKey { get; set; }

        protected abstract TRequest GetRequest();

        protected abstract TRequest GetSkipRequest(TRequest request);

        protected abstract TRequest GetFetchRequest(TRequest request);

        protected abstract RequestResponseData GetResponse(TRequest request);

        public override bool Next()
        {
            if (RemainedCount == 0 || (RemainedSkipCount != 0 && !SkipOffset()))
                return false;

            if (Items == null || Items.Count <= _index)
            {
                if (!Fetch())
                    return false;

                _index = 0;
            }

            ReadCount++;
            Current = Items[_index++];

            return true;
        }

        protected virtual bool Fetch()
        {
            if (!HasRows)
                return false;

            var request = GetFetchRequest(GetRequest());
            var response = GetResponse(request);

            Items = response.Items;

            if (response.ExclusiveStartKey.Count == 0)
                HasRows = false;

            return Items.Count != 0;
        }

        protected virtual bool SkipOffset()
        {
            while (RemainedSkipCount > 0)
            {
                var request = GetSkipRequest(GetRequest());
                var response = GetResponse(request);

                ExclusiveStartKey = response.ExclusiveStartKey;

                if (ExclusiveStartKey.Count == 0 || response.Items.Count == 0)
                    return false;

                RemainedSkipCount -= response.Items.Count;
            }

            return true;
        }
    }
}
