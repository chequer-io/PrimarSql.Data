using System.Collections.Generic;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Models;
using PrimarSql.Data.Planners;

namespace PrimarSql.Data.Requesters
{
    internal interface IRequester
    {
        AmazonDynamoDBClient Client { get; }

        SelectQueryInfo QueryInfo { get; }

        ExpressionAttributeName[] ExpressionAttributeNames { get; }

        ExpressionAttributeValue[] ExpressionAttributeValues { get; }

        string HashKeyName { get; }

        string SortKeyName { get; }

        HashKey HashKey { get; }

        SortKey SortKey { get; }

        string TableName { get; }

        string IndexName { get; }

        string FilterExpression { get; }

        bool HasRows { get; }

        Dictionary<string, AttributeValue> Current { get; }

        void SetParameters(
            AmazonDynamoDBClient client,
            SelectQueryInfo queryInfo,
            ExpressionAttributeName[] expressionAttributeNames,
            ExpressionAttributeValue[] expressionAttributeValues,
            HashKey hashKey,
            SortKey sortKey,
            string hashKeyName,
            string sortKeyName,
            string tableName,
            string indexName,
            string filterExpression
        );

        bool Next();

        long RequestCount();
    }
}
