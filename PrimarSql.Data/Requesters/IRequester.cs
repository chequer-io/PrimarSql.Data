using System.Collections.Generic;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using PrimarSql.Data.Models;

namespace PrimarSql.Data.Requesters
{
    public interface IRequester
    {
        AmazonDynamoDBClient Client { get; }
        
        SelectQueryInfo QueryInfo { get; }

        ExpressionAttributeName[] ExpressionAttributeNames { get; }

        ExpressionAttributeValue[] ExpressionAttributeValues { get; }

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
            string tableName,
            string indexName,
            string filterExpression
        );

        bool Next();
        
        /*
        
        WHERE 조건에 HASH KEY와 함께 조건이 제공되었으면, Query로 API 요청
         
        1. LIMIT이 0인 경우 데이터를 표시하지 않음 (공통 스펙)
        
        2. OFFSET의 경우 PART를 나눠서 OFFSET으로 Exclusive Key를 받아올 데이터를 결정
         
        WHERE 조건에 HASH KEY, SORT KEY가 사용된 경우 Query는 ConditionExpression에 Primary Key가 사용될 수 없으므로 Scan으로 API 요청
        
        1. LIMIT이 0인 경우 데이터 표시 X
        
        2. OFFSET의 경우 PART를 나눠서 OFFSET으로 Ex~~~
         
        */
    }
}
