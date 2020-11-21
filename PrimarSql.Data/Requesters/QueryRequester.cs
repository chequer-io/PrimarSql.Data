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
        private Dictionary<string, AttributeValue> _exclusiveStartKey;
        
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
            if (!HasRows)
                return false;
            
            var queryRequest = new QueryRequest
            {
                TableName = TableName,
                ExpressionAttributeNames = ExpressionAttributeNames.ToDictionary(kv => kv.Key, kv => kv.Value),
                ExpressionAttributeValues = ExpressionAttributeValues.ToDictionary(kv => kv.Key, kv => kv.Value),
                ScanIndexForward = !QueryInfo.OrderDescend,
                FilterExpression = string.IsNullOrWhiteSpace(FilterExpression) ? null : FilterExpression.Trim(),
                KeyConditionExpression = HashKey + (SortKey != null ? " AND " + SortKey : string.Empty),
                ExclusiveStartKey = _exclusiveStartKey,
            };

            if (!string.IsNullOrEmpty(IndexName))
                queryRequest.IndexName = IndexName;

            var queryResult = Client.QueryAsync(queryRequest).Result;
            _items = queryResult.Items;

            if (queryResult.LastEvaluatedKey.Count == 0)
                HasRows = false;
            
            return _items.Count != 0;
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
