using System.Collections.Generic;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Models;
using PrimarSql.Data.Planners;

namespace PrimarSql.Data.Requesters
{
    internal abstract class BaseRequester : IRequester
    {
        public AmazonDynamoDBClient Client { get; private set; }

        public SelectQueryInfo QueryInfo { get; private set; }

        public ExpressionAttributeName[] ExpressionAttributeNames { get; private set; }

        public ExpressionAttributeValue[] ExpressionAttributeValues { get; private set; }

        public HashKey HashKey { get; private set; }

        public SortKey SortKey { get; private set; }

        public string TableName { get; private set; }

        public string IndexName { get; private set; }
        
        public string FilterExpression { get; private set; }

        public bool HasRows { get; protected set; } = true;

        public bool HasMoreRows { get; protected set; } = true;

        public Dictionary<string, AttributeValue> Current { get; protected set; }

        public void SetParameters(
            AmazonDynamoDBClient client,
            SelectQueryInfo queryInfo,
            ExpressionAttributeName[] expressionAttributeNames,
            ExpressionAttributeValue[] expressionAttributeValues,
            HashKey hashKey,
            SortKey sortKey,
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
            TableName = tableName;
            IndexName = indexName;
            FilterExpression = filterExpression;
            
            Initialize();
        }

        protected virtual void Initialize()
        {
        }

        public abstract bool Next();
    }
}
