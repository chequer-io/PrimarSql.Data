using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Extensions;

namespace PrimarSql.Data.Requesters
{
    internal sealed class GetItemRequester : BaseRequester
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
            return NextAsync().Result;
        }

        public override async Task<bool> NextAsync(CancellationToken cancellationToken = default)
        {
            if (!HasRows)
                return false;

            var attr = new Dictionary<string, AttributeValue>
            {
                [HashKey.ExpressionAttributeName.Value] = HashKey.ExpressionAttributeValue.Value
            };

            if (SortKey != null)
                attr[SortKey.ExpressionAttributeName.Value] = SortKey.ExpressionAttributeValue.Value;

            var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, Command.CancellationTokenSource.Token).Token;
            
            var getItemResponse = await Client.GetItemAsync(new GetItemRequest
            {
                TableName = TableName,
                ConsistentRead = QueryInfo.UseStronglyConsistent,
                Key = attr
            }, cts);

            if (getItemResponse.Item.Count == 0)
                return false;

            Current = getItemResponse.Item;
            HasRows = false;

            return true;
        }

        public override long RequestCount()
        {
            return Next() ? 1 : 0;
        }
    }
}
