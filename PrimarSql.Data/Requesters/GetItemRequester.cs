using System.Collections.Generic;
using System.Linq;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Extensions;

namespace PrimarSql.Data.Requesters
{
    public class GetItemRequester : BaseRequester
    {
        protected override void Initialize()
        {
            if (QueryInfo.Limit == 0 || QueryInfo.Offset > 0)
                HasRows = false;

            if (SortKey != null) // if sort key exists
            {
                if (QueryInfo.StartSortKey != null &&
                    QueryInfo.StartSortKey.Value.ToAttributeValue().ToValue() != SortKey.ExpressionAttributeValue.Value.ToValue())
                    HasRows = false;
            }

            if (QueryInfo.StartHashKey != null)
            {
                if (!QueryInfo.StartHashKey.Value.ToAttributeValue().ToValue().Equals(HashKey.ExpressionAttributeValue.Value.ToValue()))
                    HasRows = false;
            }
        }

        public override bool Next()
        {
            if (!HasRows)
                return false;

            var attr = new Dictionary<string, AttributeValue>();
            attr[HashKey.ExpressionAttributeName.Value] = HashKey.ExpressionAttributeValue.Value;

            if (SortKey != null)
                attr[SortKey.ExpressionAttributeName.Value] = SortKey.ExpressionAttributeValue.Value;

            var getItemResponse = Client.GetItemAsync(new GetItemRequest
            {
                TableName = TableName,
                ConsistentRead = QueryInfo.UseStronglyConsistent,
                Key = attr,
            }).Result;

            if (getItemResponse.Item.Count == 0)
                return false;

            Current = getItemResponse.Item;
            HasRows = false;

            return true;
        }
    }
}
