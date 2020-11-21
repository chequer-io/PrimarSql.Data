using System.Collections.Generic;
using System.Linq;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace PrimarSql.Data.Requesters
{
    public class QueryRequester : BaseRequester
    {
        private int _remainSkip = 0;
        private List<Dictionary<string, AttributeValue>> _items;
        private int _index = 0;
        
        protected override void Initialize()
        {
            if (QueryInfo.Limit == 0)
            {
                HasRows = false;
                return;
            }

            if (QueryInfo.Limit != -1)
                _remainSkip = QueryInfo.Limit;
        }

        public override bool Next()
        {
            if (!HasRows)
                return false;

            if (_remainSkip != 0 && !SkipOffset())
                return false;

            if (_items == null || _items.Count <= _index)
            {
                if (!Fetch())
                    return false;

                _index = 0;
            }
            
            Current = _items[_index++];

            return true;
        }

        private bool Fetch()
        {
            var queryRequest = new QueryRequest
            {
                TableName = TableName,
                ExpressionAttributeNames = ExpressionAttributeNames.ToDictionary(kv => kv.Key, kv => kv.Value),
                ExpressionAttributeValues = ExpressionAttributeValues.ToDictionary(kv => kv.Key, kv => kv.Value),
                ScanIndexForward = !QueryInfo.OrderDescend,
                FilterExpression = string.IsNullOrWhiteSpace(FilterExpression) ? null : FilterExpression.Trim(),
                KeyConditionExpression = HashKey + (SortKey != null ? " AND " + SortKey : "")
            };

            if (!string.IsNullOrEmpty(IndexName))
                queryRequest.IndexName = IndexName;

            _items = Client.QueryAsync(queryRequest).Result.Items;

            if (_items.Count == 0)
                return false;
            
            return true;
        }
        
        private bool SkipOffset()
        {
            Dictionary<string, AttributeValue> exclusiveStartKey = null;

            while (_remainSkip == 0)
            {
                var request = new QueryRequest
                {
                    Limit = _remainSkip,
                    TableName = TableName,
                    IndexName = IndexName,
                    ExpressionAttributeNames = ExpressionAttributeNames.ToDictionary(kv => kv.Key, kv => kv.Value),
                    ExpressionAttributeValues = ExpressionAttributeValues.ToDictionary(kv => kv.Key, kv => kv.Value),
                    ScanIndexForward = !QueryInfo.OrderDescend,
                    Select = Select.COUNT
                };

                if (exclusiveStartKey != null)
                    request.ExclusiveStartKey = exclusiveStartKey;

                var queryResponse = Client.QueryAsync(request).Result;
                exclusiveStartKey = queryResponse.LastEvaluatedKey;

                if (exclusiveStartKey == null)
                    return false;

                _remainSkip -= queryResponse.Count;
            }

            return true;
        }
    }
}
