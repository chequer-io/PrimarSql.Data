using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Models;
using PrimarSql.Data.Planners;

namespace PrimarSql.Data.Requesters
{
    internal abstract class BaseRequester : IRequester
    {
        public PrimarSqlCommand Command { get; set; }

        public IAmazonDynamoDB Client { get; private set; }

        public SelectQueryInfo QueryInfo { get; private set; }

        public ExpressionAttributeName[] ExpressionAttributeNames { get; private set; }

        public ExpressionAttributeValue[] ExpressionAttributeValues { get; private set; }

        public string HashKeyName { get; private set; }

        public string SortKeyName { get; private set; }

        public HashKey HashKey { get; private set; }

        public SortKey SortKey { get; private set; }

        public string TableName { get; private set; }

        public string IndexName { get; private set; }

        public string FilterExpression { get; private set; }

        public bool HasRows { get; protected set; } = true;

        public bool HasMoreRows { get; protected set; } = true;

        public Dictionary<string, AttributeValue> Current { get; protected set; }

        public void SetParameters(
            IAmazonDynamoDB client,
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
        )
        {
            Client = client;
            QueryInfo = queryInfo;
            ExpressionAttributeNames = expressionAttributeNames;
            ExpressionAttributeValues = expressionAttributeValues;
            HashKey = hashKey;
            SortKey = sortKey;
            HashKeyName = hashKeyName;
            SortKeyName = sortKeyName;
            TableName = tableName;
            IndexName = indexName;
            FilterExpression = filterExpression;

            Initialize();
        }

        protected virtual void Initialize()
        {
        }

        public abstract bool Next();

        public abstract Task<bool> NextAsync(CancellationToken cancellationToken = default);

        protected bool PreventData { get; set; } = false;

        public abstract long RequestCount();
    }
}
